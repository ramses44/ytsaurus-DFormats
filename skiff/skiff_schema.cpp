#include "skiff_schema.h"

namespace DFormats {

NSkiff::TSkiffSchemaPtr SkiffSchemaFromTypeV3(NTi::TTypePtr type) {
    if (type->IsVoid() || type->IsNull()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Nothing);
    }
    if (type->IsBool()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Boolean);
    }
    if (type->IsUint8()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint8);
    }
    if (type->IsUint16() || type->IsDate()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint16);
    }
    if (type->IsUint32() ||
        type->IsTzDate() || type->IsDate32() ||
        type->IsDatetime() || type->IsTzDatetime()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint32);
    }
    if (type->IsUint64() || 
        type->IsTimestamp() || type->IsTzTimestamp() ||
        type->IsDatetime64() || type->IsTimestamp64()) {

        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64);
    }
    if (type->IsInt8()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int8);
    }
    if (type->IsInt16()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int16);
    }
    if (type->IsInt32()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int32);
    }
    if (type->IsInt64() ||
        type->IsInterval() || type->IsInterval64()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int64);
    }
    if (type->IsFloat() || type->IsDouble()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Double);
    }
    if (type->IsString() || type->IsUtf8()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32);
    }
    if (type->IsJson()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32);
    }
    if (type->IsUuid()) {
        return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint128);
    }
    if (type->IsOptional()) {
        return NSkiff::CreateVariant8Schema(
            { CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
              SkiffSchemaFromTypeV3(type->AsOptional()->GetItemType()) });
    }
    if (type->IsList()) {
        return NSkiff::CreateRepeatedVariant8Schema(
            { SkiffSchemaFromTypeV3(type->AsList()->GetItemType()) });
    }
    if (type->IsVariant()) {
        auto variant = type->AsVariant();

        if (variant->IsVariantOverTuple()) {
            return NSkiff::CreateVariant8Schema(
                SkiffSchemaFromTypeV3(variant->GetUnderlyingType()->AsTuple())->GetChildren());
        } else {
            return NSkiff::CreateVariant8Schema(
                SkiffSchemaFromTypeV3(variant->GetUnderlyingType()->AsStruct())->GetChildren());
        }
    }
    if (type->IsTuple()) {
        std::vector<NSkiff::TSkiffSchemaPtr> tupleSkiffElements;
        for (const auto& elem : type->AsTuple()->GetElements()) {
            tupleSkiffElements.emplace_back(SkiffSchemaFromTypeV3(elem.GetType()));
        }

        return NSkiff::CreateTupleSchema(std::move(tupleSkiffElements));
    }
    if (type->IsStruct()) {
        std::vector<NSkiff::TSkiffSchemaPtr> tupleSkiffElements;
        for (const auto& elem : type->AsStruct()->GetMembers()) {
            tupleSkiffElements.emplace_back(SkiffSchemaFromTypeV3(
                elem.GetType())->SetName(TString(elem.GetName())));
        }

        return NSkiff::CreateTupleSchema(std::move(tupleSkiffElements));
    }
    if (type->IsDict()) {
        auto keyType = type->AsDict()->GetKeyType();
        auto valueType = type->AsDict()->GetValueType();

        return NSkiff::CreateRepeatedVariant8Schema({ NSkiff::CreateTupleSchema(
            { SkiffSchemaFromTypeV3(keyType),
              SkiffSchemaFromTypeV3(valueType) }
        ) });
    }
    if (type->IsTagged()) {
        return SkiffSchemaFromTypeV3(type->AsTagged()->GetItemType());
    }
    
    return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Yson32);
}

NSkiff::TSkiffSchemaPtr SkiffSchemaFromTableSchema(const TTableSchema& tableSchema) {
    std::vector<NSkiff::TSkiffSchemaPtr> skiffColumns;
    for (const auto& column : tableSchema.Columns()) {
        skiffColumns.emplace_back(SkiffSchemaFromTypeV3(column.TypeV3())->SetName(column.Name()));
    }

    return NSkiff::CreateTupleSchema(std::move(skiffColumns));
}

int64_t SkiffSchemaStaticSize(const NSkiff::TSkiffSchemaPtr& schema) {
    switch (schema->GetWireType()) {
    case NSkiff::EWireType::Tuple: {
        int64_t commonSize = 0;
        for (const auto& childSchema : schema->GetChildren()) {
            auto size = SkiffSchemaStaticSize(childSchema);
            if (size == -1) return -1;
            commonSize += size;
        }
        return commonSize;
    }
    case NSkiff::EWireType::Nothing:
        return 0;
    case NSkiff::EWireType::Int8:
    case NSkiff::EWireType::Uint8:
    case NSkiff::EWireType::Boolean:
        return 1;
    case NSkiff::EWireType::Int16:
    case NSkiff::EWireType::Uint16:
        return 2;
    case NSkiff::EWireType::Int32:
    case NSkiff::EWireType::Uint32:
        return 4;
    case NSkiff::EWireType::Int64:
    case NSkiff::EWireType::Uint64:
    case NSkiff::EWireType::Double:
        return 8;
    case NSkiff::EWireType::Int128:
    case NSkiff::EWireType::Uint128:
        return 16;
    case NSkiff::EWireType::Int256:
    case NSkiff::EWireType::Uint256:
        return 32;
    default:
        return -1;
    }
}

}