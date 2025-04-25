#include "skiff_reader.h"

///// TSkiffRowReader

TSkiffRowReader::TSkiffRowReader(IInputStream* underlying,
    TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas, TOptions options)
  : Underlying_(underlying)
  , SkiffSchema_(NSkiff::CreateVariant16Schema(std::move(skiffSchemas)))
  , Options_(std::move(options)) { }

TSkiffRowReader::TSkiffRowReader(IInputStream* underlying,
    const TVector<TTableSchema>& tableSchemas, TOptions options)
  : Underlying_(underlying)
  , Options_(std::move(options)) {

    TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas;
    skiffSchemas.reserve(skiffSchemas.size());
    for (const auto& tableSchema : tableSchemas) {
        skiffSchemas.emplace_back(SkiffSchemaFromTableSchema(tableSchema));
    }

    SkiffSchema_ = NSkiff::CreateVariant16Schema(std::move(skiffSchemas));
}

template <class T>
bool TSkiffRowReader::ReadFromStream(T* dst, size_t len, bool allowEOS) {
    auto readBytes = Underlying_->Load(static_cast<void*>(dst), len);

    if (readBytes == 0 && len != 0) {
        Y_ENSURE(allowEOS, "Premature end of stream");
        return false;
    }

    Y_ENSURE(readBytes == len, "Premature end of stream " + std::to_string(readBytes) + " " + std::to_string(len));

    return true;
}

bool TSkiffRowReader::SkipFromStream(size_t len, bool allowEOS) {
    auto readBytes = Underlying_->Skip(len);

    if (readBytes == 0) {
        Y_ENSURE(allowEOS, "Premature end of stream");
        return false;
    }

    Y_ENSURE(readBytes == len, "Premature end of stream");

    return true;
}

void TSkiffRowReader::ClearContext() {
    ReadingContext_.TableIndex = -1;
    ReadingContext_.AfterKeySwitch = false;
    ReadingContext_.RowIndex = -1;
    ReadingContext_.RangeIndex = -1;
} 

void TSkiffRowReader::ReadContext() {
    if (NextTableIndex_.Defined()) {
        ReadingContext_.TableIndex = *NextTableIndex_;
        NextTableIndex_.Clear();
    } else {
        ReadFromStream(&ReadingContext_.TableIndex, 2);
    }

    if (static_cast<uint16_t>(ReadingContext_.TableIndex) == NSkiff::EndOfSequenceTag<ui16>()) {
        ythrow TIOException() << "Cannot read row - EOS";
    }

    if (static_cast<uint16_t>(ReadingContext_.TableIndex) >= SkiffSchema_->GetChildren().size()) {
        ythrow TIOException() <<
            "Table index " << static_cast<uint16_t>(ReadingContext_.TableIndex) <<
            " is out of range [0, " << SkiffSchema_->GetChildren().size() <<
            ") in read";
    }

    if (Options_.HasKeySwitch_) {
        ReadingContext_.AfterKeySwitch = false;
        ReadFromStream(&ReadingContext_.AfterKeySwitch);
    }

    uint8_t tag;
    ReadFromStream(&tag);
    if (tag == 1) {
        ReadingContext_.RowIndex = 0;
        ReadFromStream(&ReadingContext_.RowIndex);
    } else {
        Y_ENSURE(tag == 0, "Tag for row_index was expected to be 0 or 1, got " << tag);
    }

    if (Options_.HasRangeIndex_) {
        ReadFromStream(&tag);
        if (tag == 1) {
            ReadingContext_.RangeIndex = 0;
            ReadFromStream(&ReadingContext_.RangeIndex);
        } else {
            Y_ENSURE(tag == 0, "Tag for range_index was expected to be 0 or 1, got " << tag);
        }
    }
}

std::unique_ptr<TSkiffRow> TSkiffRowReader::ReadRow() {
    ReadContext();
    
    Row_ = std::make_unique<TSkiffRow>(SkiffSchema_->GetChildren() [ReadingContext_.TableIndex]);

    ReadField(Row_->Schema_);

    return std::move(Row_);
}

void TSkiffRowReader::SkipRow() {
    ReadContext();
    ReadingContext_ = TReadingContext();  // Clear read context
    SkipField(SkiffSchema_->GetChildren()[ReadingContext_.TableIndex]);
}

void TSkiffRowReader::ReadField(const NSkiff::TSkiffSchemaPtr fieldSkiffSchema, size_t nestedLevel) {
    Y_ENSURE(Row_, "Try reading data to undefined row");

    if (nestedLevel == 0) {  // Если fieldSkiffSchema - это кортеж-схема таблицы
        Row_->FieldsDataOffsets_.clear();  // Очищаем отступы, чтобы потом туда делать push_back
    } else if (nestedLevel == 1) {  // Колонки как вложенные поля кортежа-схемы таблицы
        Row_->FieldsDataOffsets_.push_back(Row_->Data_.Size());
    }

    switch (fieldSkiffSchema->GetWireType()) {
    // Simple types
    case NSkiff::EWireType::Int8:
    case NSkiff::EWireType::Uint8:
    case NSkiff::EWireType::Boolean:
        Row_->Data_.Advance(1);
        ReadFromStream(Row_->Data_.Pos() - 1, 1);
        break;
    case NSkiff::EWireType::Int16:
    case NSkiff::EWireType::Uint16:
        Row_->Data_.Advance(2);
        ReadFromStream(Row_->Data_.Pos() - 2, 2);
        break;
    case NSkiff::EWireType::Int32:
    case NSkiff::EWireType::Uint32:
        Row_->Data_.Advance(4);
        ReadFromStream(Row_->Data_.Pos() - 4, 4);
        break;
    case NSkiff::EWireType::Int64:
    case NSkiff::EWireType::Uint64:
    case NSkiff::EWireType::Double:
        Row_->Data_.Advance(8);
        ReadFromStream(Row_->Data_.Pos() - 8, 8);
        break;
    case NSkiff::EWireType::Int128:
    case NSkiff::EWireType::Uint128:
        Row_->Data_.Advance(16);
        ReadFromStream(Row_->Data_.Pos() - 16, 16);
        break;
    case NSkiff::EWireType::Int256:
    case NSkiff::EWireType::Uint256:
        Row_->Data_.Advance(32);
        ReadFromStream(Row_->Data_.Pos() - 32, 32);
        break;
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32:
        uint32_t length;
        ReadFromStream(&length);
        Row_->Data_.Append(reinterpret_cast<char*>(&length), 4);
        Row_->Data_.Advance(length);
        ReadFromStream(Row_->Data_.Pos() - length, length);
        break;

    // Complex types
    case NSkiff::EWireType::Tuple:
        for (auto childSkiffSchema : fieldSkiffSchema->GetChildren()) {
            ReadField(childSkiffSchema, nestedLevel + 1);
        }
        break;
    case NSkiff::EWireType::Variant8:
        uint8_t tag8;
        ReadFromStream(&tag8);
        Row_->Data_.Append(reinterpret_cast<char*>(&tag8), 1);
        ReadField(fieldSkiffSchema->GetChildren() [tag8], nestedLevel + 1);
        break;
    case NSkiff::EWireType::Variant16:
        uint16_t tag16;
        ReadFromStream(&tag16);
        Row_->Data_.Append(reinterpret_cast<char*>(&tag16), 2);
        ReadField(fieldSkiffSchema->GetChildren() [tag16], nestedLevel + 1);
        break;
    case NSkiff::EWireType::RepeatedVariant8:
        uint8_t rtag8;
        while (true) {
            ReadFromStream(&rtag8);
            Row_->Data_.Append(reinterpret_cast<char*>(&rtag8), 1);
            
            if (rtag8 == NSkiff::EndOfSequenceTag<ui8>()) break;

            ReadField(fieldSkiffSchema->GetChildren() [rtag8], nestedLevel + 1);
        }
        break;
    case NSkiff::EWireType::RepeatedVariant16:
        uint16_t rtag16;
        while (true) {
            ReadFromStream(&rtag16);
            Row_->Data_.Append(reinterpret_cast<char*>(&rtag16), 2);
            
            if (rtag16 == NSkiff::EndOfSequenceTag<ui8>()) break;

            ReadField(fieldSkiffSchema->GetChildren() [rtag16], nestedLevel + 1);
        }
        break;
    default:
        break;
    }
}

void TSkiffRowReader::SkipField(const NSkiff::TSkiffSchemaPtr fieldSkiffSchema) {
    switch (fieldSkiffSchema->GetWireType()) {
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
    case NSkiff::EWireType::Yson32:
        uint32_t length;
        ReadFromStream(&length);
        SkipFromStream(length);
        break;

    // Complex types
    case NSkiff::EWireType::Tuple:
        for (auto childSkiffSchema : fieldSkiffSchema->GetChildren()) {
            SkipField(childSkiffSchema);
        }
        break;
    case NSkiff::EWireType::Variant8:
        uint8_t tag8;
        ReadFromStream(&tag8);
        SkipField(fieldSkiffSchema->GetChildren() [tag8]);
        break;
    case NSkiff::EWireType::Variant16:
        uint16_t tag16;
        ReadFromStream(&tag16);
        SkipField(fieldSkiffSchema->GetChildren() [tag16]);
        break;
    case NSkiff::EWireType::RepeatedVariant8:
        uint8_t rtag8;
        while (true) {
            ReadFromStream(&rtag8);
            if (rtag8 == NSkiff::EndOfSequenceTag<ui8>()) break;
            SkipField(fieldSkiffSchema->GetChildren() [rtag8]);
        }
        break;
    case NSkiff::EWireType::RepeatedVariant16:
        uint16_t rtag16;
        while (true) {
            ReadFromStream(&rtag16);
            if (rtag16 == NSkiff::EndOfSequenceTag<ui16>()) break;
            SkipField(fieldSkiffSchema->GetChildren() [rtag16]);
        }
        break;
    default:
        break;
    }
}

void TSkiffRowReader::Next() {
    Row_ = nullptr;
    ClearContext();
    NextTableIndex_.Clear();
} 

bool TSkiffRowReader::IsValid() {
    if (!NextTableIndex_.Defined()) {
        NextTableIndex_ = 0;
        if (!ReadFromStream(&*NextTableIndex_, 2, /* allowEOS = */ true)) {
            NextTableIndex_ = NSkiff::EndOfSequenceTag<ui16>();
        }
    }

    return *NextTableIndex_ != NSkiff::EndOfSequenceTag<ui16>();
}

uint16_t TSkiffRowReader::GetTableIndex() const {
    return static_cast<uint16_t>(ReadingContext_.TableIndex);
}

int64_t TSkiffRowReader::GetRangeIndex() const {
    return ReadingContext_.RangeIndex;
}

int64_t TSkiffRowReader::GetRowIndex() const {
    return ReadingContext_.RowIndex;
}

bool TSkiffRowReader::IsAfterKeySwitch() const {
    return ReadingContext_.AfterKeySwitch;
}

const TReadingContext& TSkiffRowReader::GetReadingContext() const {
    return ReadingContext_;
}

