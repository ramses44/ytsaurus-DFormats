#pragma once

#include <util/generic/fwd.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <any>

using namespace arrow;

using TArrowSchemaPtr = std::shared_ptr<Schema>;

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

class TArrowRow {
public:
    TArrowRow(TArrowSchemaPtr arrowSchema)
      : ArrowSchema_(arrowSchema), FieldsData_(arrowSchema->num_fields()) { }
    TArrowRow(TArrowSchemaPtr arrowSchema, TVector<std::any> fieldsData)
      : ArrowSchema_(arrowSchema), FieldsData_(fieldsData) { }
    TArrowRow(TArrowRow&& rhs) {
        *this = std::move(rhs);
    }

    TArrowRow& operator=(TArrowRow&& rhs) {
        ArrowSchema_ = rhs.ArrowSchema_;
        FieldsData_ = std::move(rhs.FieldsData_);

        return *this;
    }

    const TArrowSchemaPtr& GetArrowSchema() const {
        return ArrowSchema_;
    }

    bool IsNull(size_t ind) const {
        return !FieldsData_[ind].has_value();
    }

    template<class T>
    T GetData(size_t ind) const {
        Y_ENSURE(ind < FieldsData_.size(),
                 "Invalid index. It must be less than " + std::to_string(FieldsData_.size()));
        return std::any_cast<T>(FieldsData_[ind]);
    }

    template<class T>
    void SetData(size_t ind, T value) {
        Y_ENSURE(ind < FieldsData_.size(),
                 "Invalid index. It must be less than " + std::to_string(FieldsData_.size()));
        FieldsData_[ind] = value;
    }

    template<>
    std::any GetData<std::any>(size_t ind) const {
        Y_ENSURE(ind < FieldsData_.size(),
                 "Invalid index. It must be less than " + std::to_string(FieldsData_.size()));
        return FieldsData_[ind];
    }

protected:
    TArrowSchemaPtr ArrowSchema_;
    TVector<std::any> FieldsData_;
};