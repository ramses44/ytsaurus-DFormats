#pragma once 

#include <unordered_map>

#include "protobuf_types.h"
#include "protobuf_schema.h"

using namespace google::protobuf;

namespace DFormats {

class TProtobufRowFactory {
public:
    TProtobufRowFactory(std::vector<NYT::TTableSchema> tableSchemas);
    TProtobufRowFactory(std::vector<NYT::TTableSchema> tableSchemas, std::vector<std::string> typeNames);

    TProtobufRowFactory(TProtobufRowFactory&&) = default;
    TProtobufRowFactory& operator=(TProtobufRowFactory&&) = default;

    const Descriptor* GetDescriptor(const std::string& typeName) const;
    const NYT::TTableSchema& GetTableSchema(const std::string& typeName) const;
    const std::vector<std::string>& TypeNames() const;

    std::shared_ptr<DynamicMessageFactory> RawMessageFactory() const;

    std::unique_ptr<Message> NewRawMessage(const std::string& typeName) const;
    std::unique_ptr<TProtobufRow> NewRow(const std::string& typeName) const;

protected:
    const std::unique_ptr<::DescriptorPool>& DescriptorPool() const;

private:
    std::unique_ptr<::DescriptorPool> DescriptorPool_;
    std::shared_ptr<DynamicMessageFactory> MessageFactory_;
    std::unordered_map<std::string, std::pair<NYT::TTableSchema, const Descriptor*>> Types_;
    std::vector<std::string> TypeNames_;  // For keeping order
};

}
