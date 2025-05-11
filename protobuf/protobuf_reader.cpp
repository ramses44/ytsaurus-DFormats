#include "protobuf_reader.h"

namespace DFormats {

TProtobufRowReader::TProtobufRowReader(::TIntrusivePtr<TRawTableReader> input,
    std::shared_ptr<TProtobufRowFactory> rowFactory, const std::vector<size_t>& inputTypesNumbers) {
    
    std::vector<std::string> typeNames;
    typeNames.reserve(inputTypesNumbers.size());

    for (auto num : inputTypesNumbers) {
        typeNames.emplace_back(rowFactory->TypeNames()[num]);
    }

    *this = TProtobufRowReader(input, std::move(rowFactory), std::move(typeNames));
}

TProtobufRowReader::TProtobufRowReader(::TIntrusivePtr<TRawTableReader> input, std::shared_ptr<TProtobufRowFactory> rowFactory,
    std::vector<std::string> typeNames) : RowFactory_(std::move(rowFactory)), TypeNames_(std::move(typeNames)) {

    TVector<const Descriptor*> descriptors;
    descriptors.reserve(TypeNames_.size());

    for (const auto& typeName : TypeNames_) {
        descriptors.push_back(RowFactory_->GetDescriptor(typeName));
    }

    Underlying_ = std::make_unique<TLenvalProtoTableReader>(std::move(input), std::move(descriptors));
    RefreshReadingContext();
}

bool TProtobufRowReader::IsValid() const {
    return Underlying_->IsValid();
}

bool TProtobufRowReader::IsEndOfStream() const {
    return Underlying_->IsEndOfStream();
}

void TProtobufRowReader::Next() {
    Underlying_->Next();
    RefreshReadingContext();
}

IRowPtr TProtobufRowReader::ReadRow()  {
    auto row = RowFactory_->NewRow(TypeNames_[Underlying_->GetTableIndex()]);
    Underlying_->ReadRow(row->RawMessage());
    return std::move(row);
}

std::unique_ptr<Message> TProtobufRowReader::ReadRawMessage() {
    auto message = RowFactory_->NewRawMessage(TypeNames_[Underlying_->GetTableIndex()]);
    Underlying_->ReadRow(message.get());
    return {std::move(message)};
}

const TReadingContext& TProtobufRowReader::GetReadingContext() const {
    return ReadingContext_;
}

Format TProtobufRowReader::Format() const {
    return Format::Protobuf;
}

size_t TProtobufRowReader::GetTablesCount() const {
    return TypeNames_.size();
}

const NYT::TTableSchema& TProtobufRowReader::GetTableSchema(size_t tableIndex) const {
    return RowFactory_->GetTableSchema(TypeNames_[tableIndex]);
}

std::shared_ptr<TProtobufRowFactory> TProtobufRowReader::RowFactory() const {
    return RowFactory_;
}

void TProtobufRowReader::RefreshReadingContext() {
    if (Underlying_->IsValid())
        ReadingContext_ = {Underlying_->GetTableIndex(), Underlying_->GetRowIndex(), Underlying_->GetRangeIndex(), {}};
    else
        ReadingContext_ = {};
}

}
