#include "protobuf_schema.h"

namespace DFormats {

DescriptorProto* MakeProtoStruct(FileDescriptorProto* fileProto, NTi::TStructTypePtr structType, TStringBuf name) {
    DescriptorProto* msgProto = fileProto->add_message_type();
    msgProto->set_name(name);

    size_t protoNumber = 0;
    for (const auto& member : structType->GetMembers()) {
        FieldDescriptorProto* fieldProto = msgProto->add_field();
        fieldProto->set_name(member.GetName());
        fieldProto->set_number(++protoNumber);
        InitFieldType(fileProto, fieldProto, member.GetType(), name);
    }

    return msgProto;
}

DescriptorProto* MakeMapEntry(FileDescriptorProto* fileProto, NTi::TDictTypePtr mapType, TStringBuf name) {
    DescriptorProto* msgProto = fileProto->add_message_type();
    msgProto->set_name(name);

    // MessageOptions* msgOptions = msgProto->mutable_options();
    // msgOptions->set_map_entry(true);

    FieldDescriptorProto* keyField = msgProto->add_field();
    keyField->set_name("key");
    keyField->set_number(1);
    InitFieldType(fileProto, keyField, mapType->GetKeyType(), name);
    keyField->set_label(FieldDescriptorProto::LABEL_OPTIONAL); 

    FieldDescriptorProto* valueField = msgProto->add_field();
    valueField->set_name("value");
    valueField->set_number(2);
    InitFieldType(fileProto, valueField, mapType->GetValueType(), name);
    valueField->set_label(FieldDescriptorProto::LABEL_OPTIONAL);

    return msgProto;
}

DescriptorProto* MakeProtoVariant(FileDescriptorProto* fileProto, NTi::TVariantTypePtr variantType, TStringBuf name, TStringBuf oneofFieldName) {
    DescriptorProto* msgProto = MakeProtoStruct(fileProto, variantType->GetUnderlyingType()->AsStruct(), name);

    OneofDescriptorProto* oneofProto = msgProto->add_oneof_decl();
    oneofProto->set_name(oneofFieldName);

    for (int i = 0; i < msgProto->field_size(); ++i) {
        auto fieldProto = msgProto->mutable_field(i);
        Y_ENSURE(fieldProto->label() != FieldDescriptorProto::LABEL_REPEATED, "Repeated fields are not allowed in variants");

        fieldProto->set_oneof_index(0);
        fieldProto->set_label(FieldDescriptorProto::LABEL_OPTIONAL);
    }

    OneofOptions* oneofOptions = oneofProto->mutable_options();

    oneofOptions->SetExtension(NYT::variant_field_name, oneofFieldName.data());
    oneofOptions->AddExtension(NYT::oneof_flags, NYT::EWrapperOneofFlag_Enum_VARIANT);

    return msgProto;
}

void InitFieldType(FileDescriptorProto* fileProto, FieldDescriptorProto* fieldProto, NTi::TTypePtr type, TStringBuf parentMsgName) {
    if (type->IsBool()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_BOOL);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsUint8() || type->IsUint16() || type->IsUint32()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_UINT32);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsUint64()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_UINT64);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsInt8() || type->IsInt16() || type->IsInt32()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_INT32);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsInt64()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_INT64);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsFloat()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_FLOAT);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsDouble()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_DOUBLE);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsString() || type->IsUtf8()) {
        fieldProto->set_type(FieldDescriptorProto::TYPE_STRING);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsOptional()) {
        InitFieldType(fileProto, fieldProto, type->AsOptional()->GetItemType(), parentMsgName);
        if (fieldProto->label() == FieldDescriptorProto::LABEL_REQUIRED) {
            fieldProto->set_label(FieldDescriptorProto::LABEL_OPTIONAL);
        }

    } else if (type->IsList()) {
        InitFieldType(fileProto, fieldProto, type->AsList()->GetItemType(), parentMsgName);
        Y_ENSURE(fieldProto->label() != FieldDescriptorProto::LABEL_REPEATED, "Nested lists are not supported");
        fieldProto->set_label(FieldDescriptorProto::LABEL_REPEATED);

    } else if (type->IsVariant() && type->AsVariant()->IsVariantOverStruct()) {
        auto asVariantType = type->AsVariant();

        TString variantName = TString(parentMsgName) + "_" + 
            (asVariantType->GetName().Defined() ?
             *asVariantType->GetName() :
             "Field" + std::to_string(fieldProto->number()) + "Variant");

        (void) MakeProtoVariant(fileProto, asVariantType, variantName, fieldProto->name());

        fieldProto->set_type(FieldDescriptorProto::TYPE_MESSAGE);
        fieldProto->set_type_name(variantName);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);
        fieldProto->mutable_options()->AddExtension(NYT::flags, NYT::EWrapperFieldFlag_Enum_EMBEDDED);
    } else if (type->IsStruct()) {
        auto asStructType = type->AsStruct();

        TString structName = TString(parentMsgName) + "_" + 
            (asStructType->GetName().Defined() ?
             *asStructType->GetName() :
             "Field" + std::to_string(fieldProto->number()) + "Struct");

        (void) MakeProtoStruct(fileProto, asStructType, structName);

        fieldProto->set_type(FieldDescriptorProto::TYPE_MESSAGE);
        fieldProto->set_type_name(structName);
        fieldProto->set_label(FieldDescriptorProto::LABEL_REQUIRED);

    } else if (type->IsDict()) {
        TString mapEntryName = TString(parentMsgName) + "_" + 
            "Field" + std::to_string(fieldProto->number()) + "MapEntry";
        (void) MakeMapEntry(fileProto, type->AsDict(), mapEntryName);

        fieldProto->set_label(FieldDescriptorProto::LABEL_REPEATED);
        fieldProto->set_type(FieldDescriptorProto::TYPE_MESSAGE);
        fieldProto->set_type_name(mapEntryName);

    } else {
        ythrow yexception() << "Unsupported type";
    } 
}

DescriptorPool* MakeDescriptorPool(const THashMap<TString, NYT::TTableSchema>& msgSchemas) {
    auto pool = new DescriptorPool(DescriptorPool::generated_pool());  // Use underlay for not to recompile YT extensions at runtime

    Y_ENSURE(pool->FindFileByName(kExtensionProtoFile),
             "Cannot find YT extension proto file as " << kExtensionProtoFile);
    Y_ENSURE(!pool->FindFileByName(kDynamicProtoFile), 
             "Dynamic proto file is already compiled as " << kDynamicProtoFile);

    FileDescriptorProto dynamicFileProto;
    dynamicFileProto.set_name(kDynamicProtoFile);
    dynamicFileProto.add_dependency(kExtensionProtoFile);

    FileOptions* fileOptions = dynamicFileProto.mutable_options();
    fileOptions->AddExtension(NYT::file_default_field_flags, NYT::EWrapperFieldFlag_Enum_SERIALIZATION_YT);
    fileOptions->AddExtension(NYT::file_default_field_flags, NYT::EWrapperFieldFlag_Enum_MAP_AS_LIST_OF_STRUCTS);

    for (const auto& [name, schema] : msgSchemas) {
        DescriptorProto* msgProto = dynamicFileProto.add_message_type();
        msgProto->set_name(name.c_str());

        size_t protoNumber = 0;
        for (const auto& column : schema.Columns()) {
            FieldDescriptorProto* fieldProto = msgProto->add_field();
            fieldProto->set_name(column.Name());
            fieldProto->set_number(++protoNumber);
            fieldProto->mutable_options()->SetExtension(
                (column.SortOrder().Defined() && *column.SortOrder() == NYT::ESortOrder::SO_ASCENDING ?
                 NYT::key_column_name :
                 NYT::column_name), column.Name());
            InitFieldType(&dynamicFileProto, fieldProto, column.TypeV3(), name);
        }
    }

    pool->BuildFile(dynamicFileProto);

    return pool;
}

}
