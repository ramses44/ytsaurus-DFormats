#pragma once

#include <string>
#include <memory>
#include <vector>
#include <tuple>
#include <unordered_map>

#include <library/cpp/skiff/skiff_schema.h>
#include <library/cpp/type_info/type.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <dformats/interface/types.h>
#include <dformats/skiff/skiff_schema.h>

namespace DFormats {

class TSkiffData;
class TSkiffStruct;
class TSkiffTuple;
class TSkiffList;
class TSkiffDict;
class TSkiffVariant;
class TSkiffOptional;

using TSkiffDataPtr = std::shared_ptr<TSkiffData>;
using TSkiffDataConstPtr = std::shared_ptr<const TSkiffData>;

std::string SkiffSerializeString(std::string_view str);
std::string_view SkiffDeserializeString(const char* skiffStr);

std::string SkiffSerializeTNode(const TNode& str);
TNode SkiffDeserializeTNode(const char* skiffStr);

class TSkiffData {
public:
    TSkiffData(NTi::TTypePtr schema);
    TSkiffData(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffData(const TSkiffData& rhs);
    TSkiffData(TSkiffData&& rhs);

    TSkiffData& operator=(const TSkiffData& rhs);
    TSkiffData& operator=(TSkiffData&& rhs);

    virtual ~TSkiffData() = default;

    inline NTi::TTypePtr GetSchema() const {
        return Schema_;
    }

    inline NSkiff::TSkiffSchemaPtr SkiffSchema() const {
        return SkiffSchemaFromTypeV3(Schema_);
    }

    inline virtual bool NeedRebuild() const {
        return false;
    }

    size_t Rebuild();

    TBuffer Serialize() &;
    TBuffer Serialize() const &;
    TBuffer Serialize() &&;
    TBuffer Serialize() const &&;

    inline virtual void SoftRebuild() {}
    inline virtual void HardRebuild() {
        Data_ = std::move(SerializeImpl());
    }

protected:
    inline virtual TBuffer SerializeImpl() const {
        return Data_;
    }

    inline TBuffer& Buffer() {
        return Data_;
    }
    inline const TBuffer& Buffer() const {
        return Data_;
    }

    inline static TBuffer& GetBuffer(TSkiffData& object) {
        return object.Buffer();
    }
    static const TBuffer& GetBuffer(const TSkiffData& object){
        return object.Buffer();
    }

private:
    NTi::TTypePtr Schema_;
    TBuffer Data_;
};

template <typename IndexType>
class ISkiffIndexed : virtual public IBaseIndexed<IndexType> {
protected:
    bool GetBool(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Bool, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const bool*>(GetRawDataPtr(ind));
    }
    int8_t GetInt8(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int8, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const int8_t*>(GetRawDataPtr(ind));
    }
    int16_t GetInt16(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int16, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const int16_t*>(GetRawDataPtr(ind));
    }
    int32_t GetInt32(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int32, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const int32_t*>(GetRawDataPtr(ind));
    }
    int64_t GetInt64(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int64 || type == NTi::ETypeName::Interval ||
                 type == NTi::ETypeName::Interval64, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const int64_t*>(GetRawDataPtr(ind));
    }
    uint8_t GetUInt8(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint8, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const uint8_t*>(GetRawDataPtr(ind));
    }
    uint16_t GetUInt16(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint16 || 
                 type == NTi::ETypeName::Date, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const uint16_t*>(GetRawDataPtr(ind));
    }
    uint32_t GetUInt32(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint32 || type == NTi::ETypeName::TzDate ||
                 type == NTi::ETypeName::TzDatetime || type == NTi::ETypeName::Datetime ||
                 type == NTi::ETypeName::Date32, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const uint32_t*>(GetRawDataPtr(ind));
    }
    uint64_t GetUInt64(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint64 || type == NTi::ETypeName::Timestamp ||
                 type == NTi::ETypeName::TzTimestamp || type == NTi::ETypeName::Timestamp64 ||
                 type == NTi::ETypeName::Datetime64, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const uint64_t*>(GetRawDataPtr(ind));
    }
    float GetFloat(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Float, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const float*>(GetRawDataPtr(ind));
    }
    double GetDouble(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Double, "Type missmatch while getting skiff value");

        return *reinterpret_cast<const double*>(GetRawDataPtr(ind));
    }

    std::string_view GetString(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();

        if (type == NTi::ETypeName::Uuid) {
            return {GetRawDataPtr(ind), 16};
        }

        Y_ENSURE(type == NTi::ETypeName::String || type == NTi::ETypeName::Utf8 ||
                 type == NTi::ETypeName::Json || type == NTi::ETypeName::Yson ||
                 type == NTi::ETypeName::Decimal, "Type missmatch while getting skiff value");

        return SkiffDeserializeString(GetRawDataPtr(ind));
    }
    IStructConstPtr GetStruct(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Struct, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseStruct>(GetSkiffDataPtr(ind));
    }
    ITupleConstPtr GetTuple(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Tuple, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseTuple>(GetSkiffDataPtr(ind));
    }
    IListConstPtr GetList(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::List, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseList>(GetSkiffDataPtr(ind));
    }
    IDictConstPtr GetDict(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Dict, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseDict>(GetSkiffDataPtr(ind));
    }
    IVariantConstPtr GetVariant(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Variant, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseVariant>(GetSkiffDataPtr(ind));
    }
    IOptionalConstPtr GetOptional(IndexType ind) const override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Optional, "Type missmatch while getting skiff value");

        return std::dynamic_pointer_cast<const IBaseOptional>(GetSkiffDataPtr(ind));
    }

    void SetBool(IndexType ind, bool value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Bool, "Type missmatch while setting skiff value");

        *reinterpret_cast<bool*>(GetRawDataPtr(ind)) = value;
    }
    void SetInt8(IndexType ind, int8_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int8, "Type missmatch while setting skiff value");

        *reinterpret_cast<int8_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetInt16(IndexType ind, int16_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int16, "Type missmatch while setting skiff value");

        *reinterpret_cast<int16_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetInt32(IndexType ind, int32_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int32, "Type missmatch while setting skiff value");

        *reinterpret_cast<int32_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetInt64(IndexType ind, int64_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Int64 || type == NTi::ETypeName::Interval ||
                 type == NTi::ETypeName::Interval64, "Type missmatch while setting skiff value");

        *reinterpret_cast<int64_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetUInt8(IndexType ind, uint8_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint8, "Type missmatch while setting skiff value");

        *reinterpret_cast<uint8_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetUInt16(IndexType ind, uint16_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint16 || 
                 type == NTi::ETypeName::Date, "Type missmatch while setting skiff value");

        *reinterpret_cast<uint16_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetUInt32(IndexType ind, uint32_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint32 || type == NTi::ETypeName::TzDate ||
                 type == NTi::ETypeName::TzDatetime || type == NTi::ETypeName::Datetime ||
                 type == NTi::ETypeName::Date32, "Type missmatch while setting skiff value");

        *reinterpret_cast<uint32_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetUInt64(IndexType ind, uint64_t value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Uint64 || type == NTi::ETypeName::Timestamp ||
                 type == NTi::ETypeName::TzTimestamp || type == NTi::ETypeName::Timestamp64 ||
                 type == NTi::ETypeName::Datetime64, "Type missmatch while setting skiff value");

        *reinterpret_cast<uint64_t*>(GetRawDataPtr(ind)) = value;
    }
    void SetFloat(IndexType ind, float value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Float, "Type missmatch while setting skiff value");

        *reinterpret_cast<float*>(GetRawDataPtr(ind)) = value;
    }
    void SetDouble(IndexType ind, double value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Double, "Type missmatch while setting skiff value");

        *reinterpret_cast<double*>(GetRawDataPtr(ind)) = value;
    }
    void SetString(IndexType ind, std::string_view value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();

        if (type == NTi::ETypeName::Uuid) {
            Y_ENSURE(value.size() == 16, "Invalid UUID data size");
            std::memcpy(GetRawDataPtr(ind), value.data(), 16);
            return;
        }

        Y_ENSURE(type == NTi::ETypeName::String || type == NTi::ETypeName::Utf8 ||
                 type == NTi::ETypeName::Json || type == NTi::ETypeName::Yson ||
                 type == NTi::ETypeName::Decimal, "Type missmatch while setting skiff value");

        auto serialization = SkiffSerializeString(value);
        *GetSkiffDataPtr(ind) = 
            std::move(TSkiffData(GetChildType(ind)->StripTags(), {serialization.data(), serialization.size()}));
    }
    void SetStruct(IndexType ind, IStructConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Struct, "Type missmatch while setting skiff value");

        *std::dynamic_pointer_cast<TSkiffStruct>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffStruct>(value);
    }
    void SetTuple(IndexType ind, ITupleConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Tuple, "Type missmatch while setting skiff value");

        
        *std::dynamic_pointer_cast<TSkiffTuple>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffTuple>(value);
    }
    void SetList(IndexType ind, IListConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::List, "Type missmatch while setting skiff value");

        *std::dynamic_pointer_cast<TSkiffList>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffList>(value);
    }
    void SetDict(IndexType ind, IDictConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Dict, "Type missmatch while setting skiff value");

        *std::dynamic_pointer_cast<TSkiffDict>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffDict>(value);
    }
    void SetVariant(IndexType ind, IVariantConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Variant, "Type missmatch while setting skiff value");

        *std::dynamic_pointer_cast<TSkiffVariant>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffVariant>(value);
    }
    void SetOptional(IndexType ind, IOptionalConstPtr value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Optional, "Type missmatch while setting skiff value");

        *std::dynamic_pointer_cast<TSkiffOptional>(GetSkiffDataPtr(ind)) = *std::dynamic_pointer_cast<const TSkiffOptional>(value);
        // std::cout << "SO " << std::dynamic_pointer_cast<TSkiffOptional>(GetSkiffDataPtr(ind))->HasValue() << std::endl;
    }

    void SetString(IndexType ind, std::string&& value) override {
        SetString(ind, std::string_view(value));
    }
    void SetStruct(IndexType ind, IStructPtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Struct, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }
    void SetTuple(IndexType ind, ITuplePtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Tuple, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }
    void SetList(IndexType ind, IListPtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::List, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }
    void SetDict(IndexType ind, IDictPtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Dict, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }
    void SetVariant(IndexType ind, IVariantPtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Variant, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }
    void SetOptional(IndexType ind, IOptionalPtr&& value) override {
        auto type = GetChildType(ind)->StripTags()->GetTypeName();
        Y_ENSURE(type == NTi::ETypeName::Optional, "Type missmatch while setting skiff value");

        GetSkiffDataPtr(ind) = std::dynamic_pointer_cast<TSkiffData>(value);
    }

protected:
    virtual const char* GetRawDataPtr(IndexType) const = 0;
    virtual char* GetRawDataPtr(IndexType) = 0;

    virtual TSkiffDataConstPtr GetSkiffDataPtr(IndexType) const = 0;
    virtual TSkiffDataPtr& GetSkiffDataPtr(IndexType) = 0;

    virtual NTi::TTypePtr GetChildType(IndexType) const = 0;
};

class TSkiffVariant : public TSkiffData, virtual public ISkiffIndexed<size_t>, virtual public IBaseVariant {
public:
    TSkiffVariant(NTi::TTypePtr schema);
    TSkiffVariant(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffVariant(const TSkiffVariant& rhs);
    TSkiffVariant(TSkiffVariant&& rhs);

    template <class T>
    TSkiffVariant(NTi::TTypePtr schema, uint16_t tag, T&& data);

    TSkiffVariant& operator=(const TSkiffVariant& rhs);
    TSkiffVariant& operator=(TSkiffVariant&& rhs);

    IVariantPtr CopyVariant() const override;

    bool IsTerminal() const;

    size_t VariantsCount() const override;
    size_t VariantNumber() const override;

    void EmplaceVariant(size_t number) override;

    bool NeedRebuild() const override;

protected:
    const char* GetRawDataPtr(size_t ind) const override;
    char* GetRawDataPtr(size_t ind) override;
    TSkiffDataConstPtr GetSkiffDataPtr(size_t ind) const override;
    TSkiffDataPtr& GetSkiffDataPtr(size_t ind) override;
    NTi::TTypePtr GetChildType(size_t ind) const override;

    TBuffer SerializeImpl() const override;
    void SoftRebuild() override;
    void HardRebuild() override;

    size_t TagSize() const;

    inline TSkiffDataPtr& ObjectiveValue() const {
        return ObjectiveValue_;
    }

public:
    static uint8_t Terminal8Tag();
    static uint16_t Terminal16Tag();

private:
    mutable TSkiffDataPtr ObjectiveValue_;
};

class TSkiffOptional : public TSkiffVariant, virtual public ISkiffIndexed<bool>, virtual public IBaseOptional {
public:
    TSkiffOptional(NTi::TTypePtr schema);
    TSkiffOptional(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffOptional(const TSkiffOptional& rhs);
    TSkiffOptional(TSkiffOptional&& rhs);

    template <class T>
    TSkiffOptional(NTi::TTypePtr schema, T&& data);

    TSkiffOptional& operator=(const TSkiffOptional& rhs);
    TSkiffOptional& operator=(TSkiffOptional&& rhs);

    IOptionalPtr CopyOptional() const override;

    size_t VariantsCount() const override;

    bool HasValue() const override;
    void ClearValue() override;

    void EmplaceValue() override;

protected:
    using TSkiffVariant::GetRawDataPtr;
    using TSkiffVariant::GetSkiffDataPtr;
    NTi::TTypePtr GetChildType(size_t ind) const override;

    const char* GetRawDataPtr(bool) const override;
    char* GetRawDataPtr(bool) override;
    TSkiffDataConstPtr GetSkiffDataPtr(bool) const override;
    TSkiffDataPtr& GetSkiffDataPtr(bool) override;
    NTi::TTypePtr GetChildType(bool) const override;
};

class TSkiffList : public TSkiffData, virtual public ISkiffIndexed<size_t>, virtual public IBaseList {
public:
    TSkiffList(NTi::TTypePtr schema);
    TSkiffList(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffList(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> elementsOffsets);
    TSkiffList(const TSkiffList& rhs);
    TSkiffList(TSkiffList&& rhs);

    TSkiffList& operator=(const TSkiffList& rhs);
    TSkiffList& operator=(TSkiffList&& rhs);

    IListPtr CopyList() const override;

    size_t Size() const override;
    void Clear() override;
    void PopBack() override;

    void Extend() override;

    bool NeedRebuild() const override;

protected:
    const char* GetRawDataPtr(size_t ind) const override;
    char* GetRawDataPtr(size_t ind) override;
    TSkiffDataConstPtr GetSkiffDataPtr(size_t ind) const override;
    TSkiffDataPtr& GetSkiffDataPtr(size_t ind) override;
    NTi::TTypePtr GetChildType(size_t ind) const override;

    TBuffer SerializeImpl() const override;
    void SoftRebuild() override;
    void HardRebuild() override;

    inline std::vector<TSkiffDataPtr>& ObjectiveValues() const {
        return ObjectiveValues_;
    }
    
    inline const std::vector<ptrdiff_t>& ElementsOffsets() const {
        return ElementsOffsets_;
    }

    inline std::vector<ptrdiff_t>& ElementsOffsets() {
        return ElementsOffsets_;
    }

private:
    std::vector<ptrdiff_t> ElementsOffsets_;
    mutable std::vector<TSkiffDataPtr> ObjectiveValues_;
};

class TSkiffTuple : public TSkiffData, virtual public ISkiffIndexed<size_t>, virtual public IBaseTuple {
public:
    TSkiffTuple(NTi::TTypePtr schema);
    TSkiffTuple(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffTuple(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets);

    TSkiffTuple(const TSkiffTuple& rhs);
    TSkiffTuple(TSkiffTuple&& rhs);

    TSkiffTuple& operator=(const TSkiffTuple& rhs);
    TSkiffTuple& operator=(TSkiffTuple&& rhs);

    ITuplePtr CopyTuple() const override;
    size_t FieldsCount() const override;

    bool NeedRebuild() const override;

protected:
    const char* GetRawDataPtr(size_t ind) const override;
    char* GetRawDataPtr(size_t ind) override;
    TSkiffDataConstPtr GetSkiffDataPtr(size_t ind) const override;
    TSkiffDataPtr& GetSkiffDataPtr(size_t ind) override;
    NTi::TTypePtr GetChildType(size_t ind) const override;

    TBuffer SerializeImpl() const override;
    void SoftRebuild() override;
    void HardRebuild() override;

    inline std::vector<TSkiffDataPtr>& ObjectiveValues() const {
        return ObjectiveValues_;
    }
    
    inline const std::vector<ptrdiff_t>& FieldsDataOffsets() const {
        return FieldsDataOffsets_;
    }

    inline std::vector<ptrdiff_t>& FieldsDataOffsets() {
        return FieldsDataOffsets_;
    }

private:
    std::vector<ptrdiff_t> FieldsDataOffsets_;
    mutable std::vector<TSkiffDataPtr> ObjectiveValues_;

public:
    inline void print_offsets() const {
        for (auto offset : FieldsDataOffsets()) {
            std::cout << offset << ' ';
        }
        std::cout << std::endl;
    }

    inline void print_obj_values() const {
        for (auto addr : ObjectiveValues()) {
            std::cout << addr << ' ';
        }
        std::cout << std::endl;
    }
};

class TSkiffStruct : public TSkiffTuple, virtual public ISkiffIndexed<std::string_view>, virtual public IBaseStruct {
public:
    TSkiffStruct(NTi::TTypePtr schema);
    TSkiffStruct(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffStruct(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets);

    TSkiffStruct(const TSkiffStruct& rhs);
    TSkiffStruct(TSkiffStruct&& rhs);

    TSkiffStruct& operator=(const TSkiffStruct& rhs);
    TSkiffStruct& operator=(TSkiffStruct&& rhs);

    IStructPtr CopyStruct() const override;

    std::vector<std::string> FieldsNames() const override;

    using TIndexesMap = std::unordered_map<std::string_view, size_t>;

    using TSkiffTuple::GetValue;
    using IBaseStruct::GetValue;
    using TSkiffTuple::SetValue;
    using IBaseStruct::SetValue;

protected:
    using TSkiffTuple::GetRawDataPtr;
    using TSkiffTuple::GetSkiffDataPtr;

    NTi::TTypePtr GetChildType(size_t ind) const override;

    const char* GetRawDataPtr(std::string_view ind) const override;
    char* GetRawDataPtr(std::string_view ind) override;
    TSkiffDataConstPtr GetSkiffDataPtr(std::string_view ind) const override;
    TSkiffDataPtr& GetSkiffDataPtr(std::string_view ind) override;
    NTi::TTypePtr GetChildType(std::string_view ind) const override;

    inline const TIndexesMap& IndexesMap() const {
        return IndexesMap_;
    }

    static TIndexesMap BuildIndexesMap(NTi::TStructTypePtr type);
private:
    TIndexesMap IndexesMap_;
};

class TSkiffDict : virtual public TSkiffList, virtual public IBaseDict {
public:
    template <typename... Args>
    TSkiffDict(Args... args) : TSkiffList(std::forward<Args>(args)...) { }

    template <typename... Args>
    TSkiffDict& operator=(Args... args) {
        TSkiffList::operator=(std::forward<Args>(args)...);
        return *this;
    }

    IDictPtr CopyDict() const override;

protected:
    NTi::TTypePtr GetChildType(size_t) const override;

private:
    inline IListPtr CopyList() const override {
        return TSkiffList::CopyList();
    }
};

class TSkiffRow : virtual public TSkiffStruct, virtual public IBaseRow {
public:
    TSkiffRow(NTi::TTypePtr schema);
    TSkiffRow(NTi::TTypePtr schema, TBuffer&& buf);
    TSkiffRow(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets);
    TSkiffRow(const NYT::TTableSchema& schema);
    TSkiffRow(const NYT::TTableSchema& schema, TBuffer&& buf);
    TSkiffRow(const NYT::TTableSchema& schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets);

    TSkiffRow(const TSkiffRow& rhs);
    TSkiffRow(TSkiffRow&& rhs);

    TSkiffRow& operator=(const TSkiffRow& rhs);
    TSkiffRow& operator=(TSkiffRow&& rhs);

    IRowPtr CopyRow() const override;

    using IBaseRow::GetValue;
    using IBaseRow::SetValue;

protected:
    using TSkiffStruct::GetChildType;

    NTi::TTypePtr GetChildType(size_t ind) const override;

private:
    inline ITuplePtr CopyTuple() const override {
        return TSkiffTuple::CopyTuple();
    }
    inline IStructPtr CopyStruct() const override {
        return TSkiffStruct::CopyStruct();
    }
};

}
