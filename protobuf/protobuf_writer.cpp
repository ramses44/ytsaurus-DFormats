#include "protobuf_writer.h"

namespace DFormats {

TProtobufRowWriter::TProtobufRowWriter(THolder<IProxyOutput> output,
    std::shared_ptr<TProtobufRowFactory> rowFactory, const std::vector<size_t>& outputTypesNumbers) {

    std::vector<std::string> typeNames;
    typeNames.reserve(outputTypesNumbers.size());

    for (auto num : outputTypesNumbers) {
        typeNames.emplace_back(rowFactory->TypeNames()[num]);
    }

    *this = TProtobufRowWriter(std::move(output), std::move(rowFactory), std::move(typeNames));
}

TProtobufRowWriter::TProtobufRowWriter(THolder<IProxyOutput> output,
    std::shared_ptr<TProtobufRowFactory> rowFactory, std::vector<std::string> typeNames) 
  : RowFactory_(std::move(rowFactory)), TypeNames_(std::move(typeNames)) {

    TVector<const Descriptor*> descriptors;
    descriptors.reserve(TypeNames_.size());

    for (const auto& typeName : TypeNames_) {
        descriptors.push_back(RowFactory_->GetDescriptor(typeName));
    }

    Underlying_ = std::make_unique<TLenvalProtoTableWriter>(std::move(output), std::move(descriptors));
}

void TProtobufRowWriter::WriteRow(const IRowConstPtr& row, size_t tableIndex) {
    Underlying_->AddRow(*std::dynamic_pointer_cast<const TProtobufRow>(row)->RawMessage(), tableIndex);
}

void TProtobufRowWriter::WriteRow(IRowPtr&& row, size_t tableIndex) {
    Underlying_->AddRow(std::move(*std::dynamic_pointer_cast<TProtobufRow>(row)->RawMessage()), tableIndex);
}

void TProtobufRowWriter::FinishTable(size_t tableIndex) {
    Underlying_->FinishTable(tableIndex);
}

Format TProtobufRowWriter::Format() const {
    return Format::Protobuf;
}

size_t TProtobufRowWriter::GetTablesCount() const {
    return Underlying_->GetTableCount();
}

const NYT::TTableSchema&TProtobufRowWriter:: GetTableSchema(size_t tableIndex) const {
    return RowFactory_->GetTableSchema(TypeNames_[tableIndex]);
}

IRowPtr TProtobufRowWriter::CreateObjectForWrite(size_t tableIndex) const {
    return RowFactory_->NewRow(TypeNames_[tableIndex]);
}

std::shared_ptr<TProtobufRowFactory> TProtobufRowWriter::RowFactory() const {
    return RowFactory_;
}

}