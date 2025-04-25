#include "skiff_schema.h"

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
        // Тут или String или Yson, пока не знаю
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
        // Проблема: у TTableSchema TypeV3 есть именованные и неименованные (Variant)[../library/cpp/type_info/type.h:1656],
        // А SkiffSchema поддерживает только неименованные.
        // В сериализации все равно будет все как неименованные, но для работы с данными из API хочется знать имена.
        
        auto variant = type->AsVariant();

        // TODO: В зависимости от кол-ва вариантов делать Variant8/Variant16. + поправить (тесты)[unittests/schema_converter_ut.cpp:101]
        if (variant->IsVariantOverTuple()) {
            return NSkiff::CreateVariant8Schema(
                SkiffSchemaFromTypeV3(variant->GetUnderlyingType()->AsTuple())->GetChildren());
        } else {
            return NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Yson32);
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
        // Пока не понятно, как хранить тег в скифф-типах
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