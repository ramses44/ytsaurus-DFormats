#include "skiff_reader.h"

using namespace DFormats;

///// TSkiffRowReader

TSkiffRowReader::TSkiffRowReader(::TIntrusivePtr<TRawTableReader> underlying,
                                 std::vector<TTableSchema> schemas, ReadingOptions options)
  : Underlying_(underlying)
  , TableSchemas_(std::move(schemas))
  , ReadingOptions_(options) {

    SkiffSchemas_.reserve(TableSchemas_.size());
    for (const auto& tableSchema: TableSchemas_) {
        SkiffSchemas_.push_back(SkiffSchemaFromTableSchema(tableSchema));
    }

    CalculateUnitable();
    Next();
}

template <class T>
bool TSkiffRowReader::ReadFromStream(T* dst, size_t len, bool allowEOS) {
    auto readBytes = Underlying_->Load(static_cast<void*>(dst), len);

    if (readBytes == 0 && len != 0) {
        Y_ENSURE(allowEOS, "Premature end of stream");
        return false;
    }

    Y_ENSURE(readBytes == len, "Premature end of stream. Expected " << 
        std::to_string(len) << " bytes, but only " << std::to_string(readBytes) << " can be read");

    return true;
}

bool TSkiffRowReader::SkipFromStream(size_t len, bool allowEOS) {
    auto readBytes = Underlying_->Skip(len);

    if (readBytes == 0) {
        Y_ENSURE(allowEOS, "Premature end of stream");
        return false;
    }

    Y_ENSURE(readBytes == len, "Premature end of stream. Expected " << 
        std::to_string(len) << " bytes, but only " << std::to_string(readBytes) << " can be read");

    return true;
}

void TSkiffRowReader::ReadContext() {
    if (!ReadFromStream(&ReadingContext_.TableIndex, /* len */ 2, /* allowEOS */ true) ||
        static_cast<uint16_t>(ReadingContext_.TableIndex) == NSkiff::EndOfSequenceTag<ui16>()) {
        Valid_ = false;
        EndOfStream_ = true;
        return;
    }

    Y_ENSURE(static_cast<uint16_t>(ReadingContext_.TableIndex) < GetTablesCount(), 
            "Table index " << ReadingContext_.TableIndex <<
            " is out of range [0, " << GetTablesCount() << ")");

    if (ReadingOptions_ & ReadingOptions::HasAfterKeySwitch) {
        ReadingContext_.AfterKeySwitch.emplace();
        ReadFromStream(&ReadingContext_.AfterKeySwitch.value());
    } else {
        ReadingContext_.AfterKeySwitch.reset();
    }

    uint8_t tag;
    ReadFromStream(&tag);
    if (tag == 1) {
        ReadingContext_.RowIndex.emplace();
        ReadFromStream(&ReadingContext_.RowIndex.value());
    } else {
        Y_ENSURE(tag == 0, "Tag for row_index was expected to be 0 or 1, but got " << tag);
        ReadingContext_.RowIndex.reset();
    }

    if (ReadingOptions_ & ReadingOptions::HasRangeIndex) {
        ReadFromStream(&tag);
        if (tag == 1) {
            ReadingContext_.RangeIndex.emplace();
            ReadFromStream(&ReadingContext_.RangeIndex.value());
        } else {
            Y_ENSURE(tag == 0, "Tag for range_index was expected to be 0 or 1, but got " << tag);
            ReadingContext_.RangeIndex.reset();
        }
    } else {
        ReadingContext_.RangeIndex.reset();
    }
}

IRowPtr TSkiffRowReader::ReadRow() {
    const auto& fieldSchemas = SkiffSchemas_[ReadingContext_.TableIndex]->GetChildren(); 

    TBuffer buf;
    std::vector<ptrdiff_t> fieldsOffsets = { 0 };
    fieldsOffsets.reserve(fieldSchemas.size());

    for (size_t i = 0; i < fieldSchemas.size(); ++i) {
        if (size_t canUniteBytes = Unitable_[ReadingContext_.TableIndex][i]) {
            buf.Advance(canUniteBytes);
            ReadFromStream(buf.Data() + fieldsOffsets.back(), canUniteBytes);

            for (; Unitable_[ReadingContext_.TableIndex][i]; ++i) {
                fieldsOffsets.push_back(fieldsOffsets.back() +
                    Unitable_[ReadingContext_.TableIndex][i] - 
                    Unitable_[ReadingContext_.TableIndex][i + 1]);
            }
            --i;
        } else {
            fieldsOffsets.push_back(fieldsOffsets.back() + ReadData(fieldSchemas[i], buf));
        }
    }
    fieldsOffsets.pop_back();

    Valid_ = false;

    return std::make_shared<TSkiffRow>(
        TableSchemas_[ReadingContext_.TableIndex], std::move(buf), std::move(fieldsOffsets));
}

size_t TSkiffRowReader::ReadData(const NSkiff::TSkiffSchemaPtr skiffSchema, TBuffer& dst) {
    size_t readBytesCount = 0;

    switch (skiffSchema->GetWireType()) {
    // Simple types
    case NSkiff::EWireType::Int8:
    case NSkiff::EWireType::Uint8:
    case NSkiff::EWireType::Boolean:
        dst.Advance(1);
        ReadFromStream(dst.Pos() - 1, 1);
        readBytesCount = 1;
        break;
    case NSkiff::EWireType::Int16:
    case NSkiff::EWireType::Uint16:
        dst.Advance(2);
        ReadFromStream(dst.Pos() - 2, 2);
        readBytesCount = 2;
        break;
    case NSkiff::EWireType::Int32:
    case NSkiff::EWireType::Uint32:
        dst.Advance(4);
        ReadFromStream(dst.Pos() - 4, 4);
        readBytesCount = 4;
        break;
    case NSkiff::EWireType::Int64:
    case NSkiff::EWireType::Uint64:
    case NSkiff::EWireType::Double:
        dst.Advance(8);
        ReadFromStream(dst.Pos() - 8, 8);
        readBytesCount = 8;
        break;
    case NSkiff::EWireType::Int128:
    case NSkiff::EWireType::Uint128:
        dst.Advance(16);
        ReadFromStream(dst.Pos() - 16, 16);
        readBytesCount = 16;
        break;
    case NSkiff::EWireType::Int256:
    case NSkiff::EWireType::Uint256:
        dst.Advance(32);
        ReadFromStream(dst.Pos() - 32, 32);
        readBytesCount = 32;
        break;
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32:
        uint32_t length;
        ReadFromStream(&length);
        dst.Append(reinterpret_cast<char*>(&length), 4);
        dst.Advance(length);
        ReadFromStream(dst.Pos() - length, length);
        readBytesCount = length + 4;
        break;

    // Complex types
    case NSkiff::EWireType::Tuple: {
        const auto& fields = skiffSchema->GetChildren();
        for (size_t i = 0; i < fields.size(); ++i) {
            readBytesCount += ReadData(fields[i], dst);
        }
        break;
    }
    case NSkiff::EWireType::Variant8: {
        uint8_t tag8;
        ReadFromStream(&tag8);
        dst.Append(reinterpret_cast<char*>(&tag8), 1);
        readBytesCount += 1 + ReadData(skiffSchema->GetChildren()[tag8], dst);
        break;
    }
    case NSkiff::EWireType::Variant16: {
        uint16_t tag16;
        ReadFromStream(&tag16);
        dst.Append(reinterpret_cast<char*>(&tag16), 2);
        readBytesCount += 2 + ReadData(skiffSchema->GetChildren()[tag16], dst);
        break;
    }
    case NSkiff::EWireType::RepeatedVariant8: {
        uint8_t rtag8;
        while (true) {
            ReadFromStream(&rtag8);
            dst.Append(reinterpret_cast<char*>(&rtag8), 1);
            readBytesCount += 1;
            
            if (rtag8 == NSkiff::EndOfSequenceTag<ui8>()) break;

            readBytesCount += ReadData(skiffSchema->GetChildren()[rtag8], dst);
        }
        break;
    }
    case NSkiff::EWireType::RepeatedVariant16: {
        uint16_t rtag16;
        while (true) {
            ReadFromStream(&rtag16);
            dst.Append(reinterpret_cast<char*>(&rtag16), 2);
            readBytesCount += 2;
            
            if (rtag16 == NSkiff::EndOfSequenceTag<ui8>()) break;

            readBytesCount += ReadData(skiffSchema->GetChildren()[rtag16], dst);
        }
        break;
    }
    default:
        break;
    }

    return readBytesCount;
}

void TSkiffRowReader::SkipData(const NSkiff::TSkiffSchemaPtr skiffSchema) {
    switch (skiffSchema->GetWireType()) {
    // Simple types
    case NSkiff::EWireType::Int8:
    case NSkiff::EWireType::Uint8:
    case NSkiff::EWireType::Boolean:
        SkipFromStream(1);
        break;
    case NSkiff::EWireType::Int16:
    case NSkiff::EWireType::Uint16:
        SkipFromStream(2);
        break;
    case NSkiff::EWireType::Int32:
    case NSkiff::EWireType::Uint32:
        SkipFromStream(4);
        break;
    case NSkiff::EWireType::Int64:
    case NSkiff::EWireType::Uint64:
    case NSkiff::EWireType::Double:
        SkipFromStream(8);
        break;
    case NSkiff::EWireType::Int128:
    case NSkiff::EWireType::Uint128:
        SkipFromStream(16);
        break;
    case NSkiff::EWireType::Int256:
    case NSkiff::EWireType::Uint256:
        SkipFromStream(32);
        break;
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32: {
        uint32_t length;
        ReadFromStream(&length);
        SkipFromStream(length);
        break;
    }

    // Complex types
    case NSkiff::EWireType::Tuple:
        for (auto childSkiffSchema : skiffSchema->GetChildren()) {
            SkipData(childSkiffSchema);
        }
        break;
    case NSkiff::EWireType::Variant8: {
        uint8_t tag8;
        ReadFromStream(&tag8);
        SkipData(skiffSchema->GetChildren()[tag8]);
        break;
    }
    case NSkiff::EWireType::Variant16: {
        uint16_t tag16;
        ReadFromStream(&tag16);
        SkipData(skiffSchema->GetChildren()[tag16]);
        break;
    }
    case NSkiff::EWireType::RepeatedVariant8: {
        uint8_t rtag8;
        while (true) {
            ReadFromStream(&rtag8);
            if (rtag8 == NSkiff::EndOfSequenceTag<ui8>()) break;
            SkipData(skiffSchema->GetChildren()[rtag8]);
        }
        break;
    }
    case NSkiff::EWireType::RepeatedVariant16: {
        uint16_t rtag16;
        while (true) {
            ReadFromStream(&rtag16);
            if (rtag16 == NSkiff::EndOfSequenceTag<ui16>()) break;
            SkipData(skiffSchema->GetChildren()[rtag16]);
        }
        break;
    }
    default:
        break;
    }
}

void TSkiffRowReader::Next() {
    if (!EndOfStream_ && Valid_) {
        SkipData(SkiffSchemas_[ReadingContext_.TableIndex]);
    }

    ReadContext();

    if (!EndOfStream_) {
        Valid_ = true;
    }
} 

bool TSkiffRowReader::IsValid() const {
    return Valid_;
}

bool TSkiffRowReader::IsEndOfStream() const {
    return EndOfStream_;
}

const TReadingContext& TSkiffRowReader::GetReadingContext() const {
    return ReadingContext_;
}

NSkiff::TSkiffSchemaPtr TSkiffRowReader::GetSkiffSchema(size_t tableIndex) const {
    return SkiffSchemas_[tableIndex];
}

enum Format TSkiffRowReader::Format() const {
    return Format::Skiff;
}

size_t TSkiffRowReader::GetTablesCount() const {
    return TableSchemas_.size();
}

const NYT::TTableSchema& TSkiffRowReader::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

void TSkiffRowReader::CalculateUnitable() {
    Unitable_.clear();

    for (size_t i = 0; i < GetTablesCount(); ++i) {
        const auto& columns = GetSkiffSchema(i)->GetChildren();
        Unitable_.emplace_back(columns.size() + 1, 0);

        for (long long i = columns.size() - 1; i >= 0; --i) {
            auto staticSize = SkiffSchemaStaticSize(columns[i]);
            if (staticSize != -1) {
                Unitable_.back()[i] = Unitable_.back()[i + 1] + staticSize;
            }
        }
    }
}