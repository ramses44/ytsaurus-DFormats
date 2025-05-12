#pragma once

#include <library/cpp/yson/node/node.h>
#include <library/cpp/type_info/type.h>

#include <dformats/common/util.h>
#include <dformats/interface/types.h>

using namespace NYT;

namespace DFormats {

class TYsonData;
class TYsonStruct;
class TYsonTuple;
class TYsonList;
class TYsonDict;
class TYsonVariant;
class TYsonOptional;

TNode ConstructNode(NTi::TTypePtr schema);

class TYsonData {
public:
    TYsonData();
    TYsonData(NTi::TTypePtr schema);
    TYsonData(NTi::TTypePtr schema, TNode underlying);

    TYsonData(const TYsonData& rhs);
    TYsonData(TYsonData&& rhs);

    virtual ~TYsonData() = default;
    
    TYsonData& operator=(const TYsonData& rhs);
    TYsonData& operator=(TYsonData&& rhs);

    const TNode& Underlying() const;
    TNode& Underlying();
    TNode&& Release();

    NTi::TTypePtr GetSchema() const;

private:
    NTi::TTypePtr Schema_;
    TNode Underlying_;
};

template <typename IndexType>
class IYsonIndexed : virtual public IBaseIndexed<IndexType> {
protected:
    bool GetBool(IndexType ind) const override {
        return GetData(ind).AsBool();
    }
    int8_t GetInt8(IndexType ind) const override {
        return GetData(ind).AsInt64();
    }
    int16_t GetInt16(IndexType ind) const override {
        return GetData(ind).AsInt64();
    }
    int32_t GetInt32(IndexType ind) const override {
        return GetData(ind).AsInt64();
    }
    int64_t GetInt64(IndexType ind) const override {
        return GetData(ind).AsInt64();
    }
    uint8_t GetUInt8(IndexType ind) const override {
        return GetData(ind).AsUint64();
    }
    uint16_t GetUInt16(IndexType ind) const override {
        return GetData(ind).AsUint64();
    }
    uint32_t GetUInt32(IndexType ind) const override {
        return GetData(ind).AsUint64();
    }
    uint64_t GetUInt64(IndexType ind) const override {
        return GetData(ind).AsUint64();
    }
    float GetFloat(IndexType ind) const override {
        return GetData(ind).AsDouble();
    }
    double GetDouble(IndexType ind) const override {
        return GetData(ind).AsDouble();
    }
    std::string_view GetString(IndexType ind) const override {
        return {GetData(ind).AsString()};
    }
    IStructConstPtr GetStruct(IndexType ind) const override {
        return std::make_shared<TYsonStruct>(GetSchema(ind), GetData(ind));
    }
    ITupleConstPtr GetTuple(IndexType ind) const override {
        return std::make_shared<TYsonTuple>(GetSchema(ind), GetData(ind));
    }
    IListConstPtr GetList(IndexType ind) const override {
        return std::make_shared<TYsonList>(GetSchema(ind), GetData(ind));
    }
    IDictConstPtr GetDict(IndexType ind) const override {
        return std::make_shared<TYsonDict>(GetSchema(ind), GetData(ind));
    }
    IVariantConstPtr GetVariant(IndexType ind) const override {
        return std::make_shared<TYsonVariant>(GetSchema(ind), GetData(ind));
    }
    IOptionalConstPtr GetOptional(IndexType ind) const override {
        return std::make_shared<TYsonOptional>(GetSchema(ind), GetData(ind));
    }

    void SetBool(IndexType ind, bool value) override {
        GetData(ind) = TNode(value);
    }
    void SetInt8(IndexType ind, int8_t value) override {
        GetData(ind) = TNode(value);
    }
    void SetInt16(IndexType ind, int16_t value) override {
        GetData(ind) = TNode(value);
    }
    void SetInt32(IndexType ind, int32_t value) override {
        GetData(ind) = TNode(value);
    }
    void SetInt64(IndexType ind, int64_t value) override {
        GetData(ind) = TNode(value);
    }
    void SetUInt8(IndexType ind, uint8_t value) override {
        GetData(ind) = TNode(static_cast<uint64_t>(value));
    }
    void SetUInt16(IndexType ind, uint16_t value) override {
        GetData(ind) = TNode(static_cast<uint64_t>(value));
    }
    void SetUInt32(IndexType ind, uint32_t value) override {
        GetData(ind) = TNode(static_cast<uint64_t>(value));
    }
    void SetUInt64(IndexType ind, uint64_t value) override {
        GetData(ind) = TNode(value);
    }
    void SetFloat(IndexType ind, float value) override {
        GetData(ind) = TNode(value);
    }
    void SetDouble(IndexType ind, double value) override {
        GetData(ind) = TNode(value);
    }
    void SetString(IndexType ind, std::string_view value) override {
        GetData(ind) = TNode(value);
    }
    void SetStruct(IndexType ind, IStructConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }
    void SetTuple(IndexType ind, ITupleConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }
    void SetList(IndexType ind, IListConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }
    void SetDict(IndexType ind, IDictConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }
    void SetVariant(IndexType ind, IVariantConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }
    void SetOptional(IndexType ind, IOptionalConstPtr value) override {
        GetData(ind) = std::dynamic_pointer_cast<const TYsonData>(value)->Underlying();
    }

    void SetString(IndexType ind, std::string&& value) override {
        GetData(ind) = TNode(std::move(value));
    }
    void SetStruct(IndexType ind, IStructPtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }
    void SetTuple(IndexType ind, ITuplePtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }
    void SetList(IndexType ind, IListPtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }
    void SetDict(IndexType ind, IDictPtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }
    void SetVariant(IndexType ind, IVariantPtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }
    void SetOptional(IndexType ind, IOptionalPtr&& value) override {
        GetData(ind) = std::dynamic_pointer_cast<TYsonData>(value)->Release();
    }

protected:
    virtual NTi::TTypePtr GetSchema(IndexType ind) const = 0; 
    virtual const TNode& GetData(IndexType ind) const = 0;
    virtual TNode& GetData(IndexType ind) = 0;
};

class TYsonStruct : public TYsonData, virtual public IYsonIndexed<std::string_view>, virtual public IBaseStruct {
public:
    TYsonStruct();
    TYsonStruct(NTi::TTypePtr schema);
    TYsonStruct(NTi::TTypePtr schema, TNode underlying);

    TYsonStruct(const TYsonStruct& rhs);
    TYsonStruct(TYsonStruct&& rhs);
    
    TYsonStruct& operator=(const TYsonStruct& rhs);
    TYsonStruct& operator=(TYsonStruct&& rhs);

    IStructPtr CopyStruct() const override;
    std::vector<std::string> FieldsNames() const override;

    using TYsonData::GetSchema;

protected:
    NTi::TTypePtr GetSchema(std::string_view ind) const override;
    const TNode& GetData(std::string_view ind) const override;
    TNode& GetData(std::string_view ind) override;
};

class TYsonTuple : public TYsonData, virtual public IYsonIndexed<size_t>, virtual public IBaseTuple {
public:
    TYsonTuple();
    TYsonTuple(NTi::TTypePtr schema);
    TYsonTuple(NTi::TTypePtr schema, TNode underlying);

    TYsonTuple(const TYsonTuple& rhs);
    TYsonTuple(TYsonTuple&& rhs);
    
    TYsonTuple& operator=(const TYsonTuple& rhs);
    TYsonTuple& operator=(TYsonTuple&& rhs);

    ITuplePtr CopyTuple() const override;
    size_t FieldsCount() const override;

    using TYsonData::GetSchema;

protected:
    NTi::TTypePtr GetSchema(size_t ind) const override;
    const TNode& GetData(size_t ind) const override;
    TNode& GetData(size_t ind) override;
};

class TYsonList : public TYsonData, virtual public IYsonIndexed<size_t>, virtual public IBaseList {
public:
    TYsonList();
    TYsonList(NTi::TTypePtr schema);
    TYsonList(NTi::TTypePtr schema, TNode underlying);

    TYsonList(const TYsonList& rhs);
    TYsonList(TYsonList&& rhs);
    
    TYsonList& operator=(const TYsonList& rhs);
    TYsonList& operator=(TYsonList&& rhs);

    IListPtr CopyList() const override;

    size_t Size() const override;
    void Clear() override;
    void PopBack() override;
    void Extend() override;

    using TYsonData::GetSchema;

protected:
    NTi::TTypePtr GetSchema(size_t ind) const override;
    const TNode& GetData(size_t ind) const override;
    TNode& GetData(size_t ind) override;
};

class TYsonVariant : public TYsonData, public IYsonIndexed<std::string_view>,
                     virtual public IYsonIndexed<size_t>, virtual public IBaseVariant {
public:
    TYsonVariant();
    TYsonVariant(NTi::TTypePtr schema);
    TYsonVariant(NTi::TTypePtr schema, TNode underlying);

    TYsonVariant(const TYsonVariant& rhs);
    TYsonVariant(TYsonVariant&& rhs);
    
    TYsonVariant& operator=(const TYsonVariant& rhs);
    TYsonVariant& operator=(TYsonVariant&& rhs);

    IVariantPtr CopyVariant() const override;

    size_t VariantsCount() const override;
    size_t VariantNumber() const override;
    void EmplaceVariant(size_t number) override;

    using TYsonData::GetSchema;

protected:
    NTi::TTypePtr GetSchema(size_t ind) const override;
    NTi::TTypePtr GetSchema(std::string_view ind) const override;
    const TNode& GetData(size_t ind) const override;
    TNode& GetData(size_t ind) override;
    const TNode& GetData(std::string_view ind) const override;
    TNode& GetData(std::string_view ind) override;
};

class TYsonOptional : public TYsonData, virtual public IYsonIndexed<bool>, virtual public IBaseOptional {
public:
    TYsonOptional();
    TYsonOptional(NTi::TTypePtr schema);
    TYsonOptional(NTi::TTypePtr schema, TNode underlying);

    TYsonOptional(const TYsonOptional& rhs);
    TYsonOptional(TYsonOptional&& rhs);
    
    TYsonOptional& operator=(const TYsonOptional& rhs);
    TYsonOptional& operator=(TYsonOptional&& rhs);

    IOptionalPtr CopyOptional() const override;

    bool HasValue() const override;
    void ClearValue() override;
    void EmplaceValue() override;  

    using TYsonData::GetSchema;

protected:
    NTi::TTypePtr GetSchema(bool) const override;
    const TNode& GetData(bool) const override;
    TNode& GetData(bool) override;
};

class TYsonDict : virtual public TYsonList, virtual public IBaseDict {
public:
    TYsonDict();
    TYsonDict(NTi::TTypePtr schema);
    TYsonDict(NTi::TTypePtr schema, TNode underlying);

    TYsonDict(const TYsonDict& rhs);
    TYsonDict(TYsonDict&& rhs);
    
    TYsonDict& operator=(const TYsonDict& rhs);
    TYsonDict& operator=(TYsonDict&& rhs);

    IDictPtr CopyDict() const override;

    void Extend() override;

    using TYsonData::GetSchema;
    using TYsonList::GetSchema;

protected:
    // For correct std::map and std::unordered_map convertion (IBaseDict)
    IStructConstPtr GetStruct(size_t ind) const override;
    void SetStruct(size_t ind, IStructConstPtr value) override;
    void SetStruct(size_t ind, IStructPtr&& value) override;

private:
    inline IListPtr CopyList() const override {
        return TYsonList::CopyList();
    }
};

class TYsonRow : virtual public TYsonStruct, virtual public IYsonIndexed<size_t>, virtual public IBaseRow {
public:
    TYsonRow();
    TYsonRow(NTi::TTypePtr schema);
    TYsonRow(NTi::TTypePtr schema, TNode underlying);
    TYsonRow(const NYT::TTableSchema& schema);
    TYsonRow(const NYT::TTableSchema& schema, TNode underlying);

    TYsonRow(const TYsonRow& rhs);
    TYsonRow(TYsonRow&& rhs);
    
    TYsonRow& operator=(const TYsonRow& rhs);
    TYsonRow& operator=(TYsonRow&& rhs);

    IRowPtr CopyRow() const override;

    using TYsonData::GetSchema;
    using TYsonStruct::GetSchema;

protected:
    using TYsonStruct::GetData;
    
    NTi::TTypePtr GetSchema(size_t ind) const override;
    const TNode& GetData(size_t ind) const override;
    TNode& GetData(size_t ind) override;

protected:
    inline size_t FieldsCount() const override {
        return Underlying().Size();
    }
    inline ITuplePtr CopyTuple() const override {
        return nullptr;
    }
    inline IStructPtr CopyStruct() const override {
        return TYsonStruct::CopyStruct();
    }
};

}
