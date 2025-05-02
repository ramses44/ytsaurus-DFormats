#pragma once

#include <dformats/interface/types.h>

namespace DFormats {

template<typename T1, typename T2>
class IIndexedProxy : virtual public IBaseIndexed<T1>, virtual public IBaseIndexed<T2> {
protected:
    virtual T2 GetIndex(T1 ind) const = 0;

protected:
    using IBaseIndexed<T2>::GetBool;
    using IBaseIndexed<T2>::GetInt8;
    using IBaseIndexed<T2>::GetInt16;
    using IBaseIndexed<T2>::GetInt32;
    using IBaseIndexed<T2>::GetInt64;
    using IBaseIndexed<T2>::GetUInt8;
    using IBaseIndexed<T2>::GetUInt16;
    using IBaseIndexed<T2>::GetUInt32;
    using IBaseIndexed<T2>::GetUInt64;
    using IBaseIndexed<T2>::GetFloat;
    using IBaseIndexed<T2>::GetDouble;
    using IBaseIndexed<T2>::GetString;
    using IBaseIndexed<T2>::GetStruct;
    using IBaseIndexed<T2>::GetTuple;
    using IBaseIndexed<T2>::GetDict;
    using IBaseIndexed<T2>::GetList;
    using IBaseIndexed<T2>::GetVariant;
    using IBaseIndexed<T2>::GetOptional;
    
    using IBaseIndexed<T2>::SetBool;
    using IBaseIndexed<T2>::SetInt8;
    using IBaseIndexed<T2>::SetInt16;
    using IBaseIndexed<T2>::SetInt32;
    using IBaseIndexed<T2>::SetInt64;
    using IBaseIndexed<T2>::SetUInt8;
    using IBaseIndexed<T2>::SetUInt16;
    using IBaseIndexed<T2>::SetUInt32;
    using IBaseIndexed<T2>::SetUInt64;
    using IBaseIndexed<T2>::SetFloat;
    using IBaseIndexed<T2>::SetDouble;
    using IBaseIndexed<T2>::SetString;
    using IBaseIndexed<T2>::SetStruct;
    using IBaseIndexed<T2>::SetTuple;
    using IBaseIndexed<T2>::SetDict;
    using IBaseIndexed<T2>::SetList;
    using IBaseIndexed<T2>::SetVariant;
    using IBaseIndexed<T2>::SetOptional;

    bool GetBool(T1 ind) const override {
        return GetBool(GetIndex(ind));
    }
    int8_t GetInt8(T1 ind) const override {
        return GetInt8(GetIndex(ind));
    }
    int16_t GetInt16(T1 ind) const override {
        return GetInt16(GetIndex(ind));
    }
    int32_t GetInt32(T1 ind) const override {
        return GetInt32(GetIndex(ind));
    }
    int64_t GetInt64(T1 ind) const override {
        return GetInt64(GetIndex(ind));
    }
    uint8_t GetUInt8(T1 ind) const override {
        return GetUInt8(GetIndex(ind));
    }
    uint16_t GetUInt16(T1 ind) const override {
        return GetUInt16(GetIndex(ind));
    }
    uint32_t GetUInt32(T1 ind) const override {
        return GetUInt32(GetIndex(ind));
    }
    uint64_t GetUInt64(T1 ind) const override {
        return GetUInt64(GetIndex(ind));
    }
    float GetFloat(T1 ind) const override {
        return GetFloat(GetIndex(ind));
    }
    double GetDouble(T1 ind) const override {
        return GetDouble(GetIndex(ind));
    }
    std::string_view GetString(T1 ind) const override {
        return GetString(GetIndex(ind));
    }
    IStructConstPtr GetStruct(T1 ind) const override {
        return GetStruct(GetIndex(ind));
    }
    ITupleConstPtr GetTuple(T1 ind) const override {
        return GetTuple(GetIndex(ind));
    }
    IListConstPtr GetList(T1 ind) const override {
        return GetList(GetIndex(ind));
    }
    IDictConstPtr GetDict(T1 ind) const override {
        return GetDict(GetIndex(ind));
    }
    IVariantConstPtr GetVariant(T1 ind) const override {
        return GetVariant(GetIndex(ind));
    }
    IOptionalConstPtr GetOptional(T1 ind) const override {
        return GetOptional(GetIndex(ind));
    }

    void SetBool(T1 ind, bool value) override {
        SetBool(GetIndex(ind), value);
    }
    void SetInt8(T1 ind, int8_t value) override {
        SetInt8(GetIndex(ind), value);
    }
    void SetInt16(T1 ind, int16_t value) override {
        SetInt16(GetIndex(ind), value);
    }
    void SetInt32(T1 ind, int32_t value) override {
        SetInt32(GetIndex(ind), value);
    }
    void SetInt64(T1 ind, int64_t value) override {
        SetInt64(GetIndex(ind), value);
    }
    void SetUInt8(T1 ind, uint8_t value) override {
        SetUInt8(GetIndex(ind), value);
    }
    void SetUInt16(T1 ind, uint16_t value) override {
        SetUInt16(GetIndex(ind), value);
    }
    void SetUInt32(T1 ind, uint32_t value) override {
        SetUInt32(GetIndex(ind), value);
    }
    void SetUInt64(T1 ind, uint64_t value) override {
        SetUInt64(GetIndex(ind), value);
    }
    void SetFloat(T1 ind, float value) override {
        SetFloat(GetIndex(ind), value);
    }
    void SetDouble(T1 ind, double value) override {
        SetDouble(GetIndex(ind), value);
    }
    void SetString(T1 ind, std::string_view value) override {
        SetString(GetIndex(ind), value);
    }
    void SetStruct(T1 ind, IStructConstPtr value) override {
        SetStruct(GetIndex(ind), value);
    }
    void SetTuple(T1 ind, ITupleConstPtr value) override {
        SetTuple(GetIndex(ind), value);
    }
    void SetList(T1 ind, IListConstPtr value) override {
        SetList(GetIndex(ind), value);
    }
    void SetDict(T1 ind, IDictConstPtr value) override {
        SetDict(GetIndex(ind), value);
    }
    void SetVariant(T1 ind, IVariantConstPtr value) override {
        SetVariant(GetIndex(ind), value);
    }
    void SetOptional(T1 ind, IOptionalConstPtr value) override {
        SetOptional(GetIndex(ind), value);
    }

    void SetString(T1 ind, std::string&& value) override {
        SetString(GetIndex(ind), std::move(value));
    }
    void SetStruct(T1 ind, IStructPtr&& value) override {
        SetStruct(GetIndex(ind), std::move(value));
    }
    void SetTuple(T1 ind, ITuplePtr&& value) override {
        SetTuple(GetIndex(ind), std::move(value));
    }
    void SetList(T1 ind, IListPtr&& value) override {
        SetList(GetIndex(ind), std::move(value));
    }
    void SetDict(T1 ind, IDictPtr&& value) override {
        SetDict(GetIndex(ind), std::move(value));
    }
    void SetVariant(T1 ind, IVariantPtr&& value) override {
        SetVariant(GetIndex(ind), std::move(value));
    }
    void SetOptional(T1 ind, IOptionalPtr&& value) override {
        SetOptional(GetIndex(ind), std::move(value));
    }
};

}