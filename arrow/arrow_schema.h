#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <yt/cpp/mapreduce/interface/io.h>

using namespace NYT;
using namespace arrow;

namespace DFormats {

using TArrowSchemaPtr = std::shared_ptr<Schema>;

inline std::shared_ptr<Schema> MakeArrowSchema(const TTableSchema& tableSchema) {
    FieldVector fields;
    fields.reserve(tableSchema.Columns().size());

    for (const auto& column : tableSchema.Columns()) {
        std::shared_ptr<DataType> type;

        switch (column.Type()) {
        case EValueType::VT_INT8:
            type = std::make_shared<Int8Type>();
            break;
        case EValueType::VT_INT16:
            type = std::make_shared<Int16Type>();
            break;
        case EValueType::VT_INT32:
            type = std::make_shared<Int32Type>();
            break;
        case EValueType::VT_INT64:
        case EValueType::VT_INTERVAL:
        case EValueType::VT_INTERVAL64:
            type = std::make_shared<Int64Type>();
            break;
        case EValueType::VT_UINT8:
            type = std::make_shared<UInt8Type>();
            break;
        case EValueType::VT_UINT16:
        case EValueType::VT_DATE:
            type = std::make_shared<UInt16Type>();
            break;
        case EValueType::VT_UINT32:
        case EValueType::VT_DATETIME:
        case EValueType::VT_DATE32:
            type = std::make_shared<UInt32Type>();
            break;
        case EValueType::VT_UINT64:
        case EValueType::VT_TIMESTAMP:
        case EValueType::VT_DATETIME64:
        case EValueType::VT_TIMESTAMP64:
            type = std::make_shared<UInt64Type>();
            break;
        case EValueType::VT_FLOAT:
            type = std::make_shared<FloatType>();
            break;
        case EValueType::VT_DOUBLE:
            type = std::make_shared<DoubleType>();
            break;
        case EValueType::VT_BOOLEAN:
            type = std::make_shared<BooleanType>();
            break;
        case EValueType::VT_UTF8:
            type = std::make_shared<StringType>();
            break;
        case EValueType::VT_VOID:
        case EValueType::VT_NULL:
            type = std::make_shared<NullType>();
            break;
        default:
            type = std::make_shared<BinaryType>();
            break;
        }

        fields.push_back(std::make_shared<Field>(column.Name(), std::move(type)));
    }

    return std::make_shared<Schema>(std::move(fields));
}

inline bool IsReadingContextColumnName(std::string_view name) {
    return name == "$table_index" || name == "$range_index" || 
           name == "$row_index" || name == "$key_switch";
}

inline TArrowSchemaPtr RemoveReadingContextColumns(TArrowSchemaPtr schema) {
    ::FieldVector fields;
    fields.reserve(schema->num_fields());
    
    for (const auto& field : schema->fields()) {
        if (!IsReadingContextColumnName(field->name())) {
            fields.push_back(field);
        }
    }

    return std::make_shared<Schema>(std::move(fields));
}

inline TArrowSchemaPtr TransformDatetimeColumns(TArrowSchemaPtr schema) {
    for (int i = 0; i < schema->num_fields(); ++i) {
        if (schema->field(i)->type()->id() == Type::DATE32) {
            schema = schema->SetField(i, std::make_shared<Field>(schema->field(i)->type()->name(), std::make_shared<Int32Type>())).ValueOr(nullptr);
        } else if (schema->field(i)->type()->id() == Type::DATE64) {
            schema = schema->SetField(i, std::make_shared<Field>(schema->field(i)->type()->name(), std::make_shared<Int64Type>())).ValueOr(nullptr);
        } else if (schema->field(i)->type()->id() == Type::TIMESTAMP) {
            schema = schema->SetField(i, std::make_shared<Field>(schema->field(i)->type()->name(), std::make_shared<UInt64Type>())).ValueOr(nullptr);
        } else if (schema->field(i)->type()->id() == Type::DICTIONARY) {
            schema = schema->SetField(i, std::make_shared<Field>(schema->field_names()[i], std::static_pointer_cast<DictionaryType>(schema->field(i)->type())->value_type())).ValueOr(nullptr);
        }
    }

    return schema;
}

}
