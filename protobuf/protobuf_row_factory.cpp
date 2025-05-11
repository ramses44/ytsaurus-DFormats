#include "protobuf_row_factory.h"

namespace DFormats {

TProtobufRowFactory::TProtobufRowFactory(std::vector<NYT::TTableSchema> tableSchemas) {
    std::vector<std::string> typeNames;
    typeNames.reserve(tableSchemas.size());

    for (size_t i = 0; i < tableSchemas.size(); ++i) {
        typeNames.push_back("DynamicMessage" + std::to_string(i));
    }

    *this = TProtobufRowFactory(std::move(tableSchemas), std::move(typeNames));
}

TProtobufRowFactory::TProtobufRowFactory(std::vector<NYT::TTableSchema> tableSchemas, std::vector<std::string> typeNames)
  : TypeNames_(std::move(typeNames)) {
    Y_ENSURE(tableSchemas.size() == TypeNames_.size(), "Type's names list and schemas list have different sizes");

    THashMap<TString, NYT::TTableSchema> schemaMap;
    
    for (size_t i = 0; i < tableSchemas.size(); ++i) {
        schemaMap[TypeNames_[i]] = std::move(tableSchemas[i]);
    }

    DescriptorPool_.reset(MakeDescriptorPool(schemaMap));
    MessageFactory_ = std::make_shared<DynamicMessageFactory>(DescriptorPool_.get());

    for (const auto& typeName : TypeNames_) {
        Types_[typeName] = std::make_pair<NYT::TTableSchema, const Descriptor*>(
            std::move(schemaMap[typeName]), DescriptorPool_->FindMessageTypeByName(typeName));
    }
}

const Descriptor* TProtobufRowFactory::GetDescriptor(const std::string& typeName) const {
    return Types_.at(typeName).second;
}

const NYT::TTableSchema& TProtobufRowFactory::GetTableSchema(const std::string& typeName) const {
    return Types_.at(typeName).first;
}

const std::vector<std::string>& TProtobufRowFactory::TypeNames() const {
    return TypeNames_;
}

std::shared_ptr<DynamicMessageFactory> TProtobufRowFactory::RawMessageFactory() const {
    return MessageFactory_;
}

std::unique_ptr<Message> TProtobufRowFactory::NewRawMessage(const std::string& typeName) const {
    return std::unique_ptr<Message>(MessageFactory_->GetPrototype(GetDescriptor(typeName))->New());
}

std::unique_ptr<TProtobufRow> TProtobufRowFactory::NewRow(const std::string& typeName) const {
    return std::make_unique<TProtobufRow>(NewRawMessage(typeName), RawMessageFactory());
}

const std::unique_ptr<::DescriptorPool>& TProtobufRowFactory::DescriptorPool() const {
    return DescriptorPool_;
}

}
