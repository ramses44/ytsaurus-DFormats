#pragma once

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <optional>
#include <variant>
#include <map>
#include <unordered_map>

#include <util/generic/yexception.h>

namespace DFormats {

class IBaseRow;
class IBaseStruct;
class IBaseTuple;
class IBaseList;
class IBaseDict;
class IBaseVariant;
class IBaseOptional;

using IRowPtr = std::shared_ptr<IBaseRow>;
using IStructPtr = std::shared_ptr<IBaseStruct>;
using ITuplePtr = std::shared_ptr<IBaseTuple>;
using IListPtr = std::shared_ptr<IBaseList>;
using IDictPtr = std::shared_ptr<IBaseDict>;
using IVariantPtr = std::shared_ptr<IBaseVariant>;
using IOptionalPtr = std::shared_ptr<IBaseOptional>;

using IRowConstPtr = std::shared_ptr<const IBaseRow>;
using IStructConstPtr = std::shared_ptr<const IBaseStruct>;
using ITupleConstPtr = std::shared_ptr<const IBaseTuple>;
using IListConstPtr = std::shared_ptr<const IBaseList>;
using IDictConstPtr = std::shared_ptr<const IBaseDict>;
using IVariantConstPtr = std::shared_ptr<const IBaseVariant>;
using IOptionalConstPtr = std::shared_ptr<const IBaseOptional>;

template<typename T>
concept IsComplexTypePtr = 
    std::is_same_v<IStructPtr, T> || std::is_same_v<ITuplePtr, T> || 
    std::is_same_v<IListPtr, T> || std::is_same_v<IDictPtr, T> || 
    std::is_same_v<IVariantPtr, T> || std::is_same_v<IOptionalPtr, T>;

template <typename IndexType>
class IBaseIndexed {
public:
    virtual ~IBaseIndexed() {}
protected:
    virtual bool GetBool(IndexType) const = 0;
    virtual int8_t GetInt8(IndexType) const = 0;
    virtual int16_t GetInt16(IndexType) const = 0;
    virtual int32_t GetInt32(IndexType) const = 0;
    virtual int64_t GetInt64(IndexType) const = 0;
    virtual uint8_t GetUInt8(IndexType) const = 0;
    virtual uint16_t GetUInt16(IndexType) const = 0;
    virtual uint32_t GetUInt32(IndexType) const = 0;
    virtual uint64_t GetUInt64(IndexType) const = 0;
    virtual float GetFloat(IndexType) const = 0;
    virtual double GetDouble(IndexType) const = 0;
    virtual std::string_view GetString(IndexType) const = 0;
    virtual IStructConstPtr GetStruct(IndexType) const = 0;
    virtual ITupleConstPtr GetTuple(IndexType) const = 0;
    virtual IListConstPtr GetList(IndexType) const = 0;
    virtual IDictConstPtr GetDict(IndexType) const = 0;
    virtual IVariantConstPtr GetVariant(IndexType) const = 0;
    virtual IOptionalConstPtr GetOptional(IndexType) const = 0;

    virtual void SetBool(IndexType, bool) = 0;
    virtual void SetInt8(IndexType, int8_t) = 0;
    virtual void SetInt16(IndexType, int16_t) = 0;
    virtual void SetInt32(IndexType, int32_t) = 0;
    virtual void SetInt64(IndexType, int64_t) = 0;
    virtual void SetUInt8(IndexType, uint8_t) = 0;
    virtual void SetUInt16(IndexType, uint16_t) = 0;
    virtual void SetUInt32(IndexType, uint32_t) = 0;
    virtual void SetUInt64(IndexType, uint64_t) = 0;
    virtual void SetFloat(IndexType, float) = 0;
    virtual void SetDouble(IndexType, double) = 0;
    virtual void SetString(IndexType, std::string_view) = 0;
    virtual void SetStruct(IndexType, IStructConstPtr) = 0;
    virtual void SetTuple(IndexType, ITupleConstPtr) = 0;
    virtual void SetList(IndexType, IListConstPtr) = 0;
    virtual void SetDict(IndexType, IDictConstPtr) = 0;
    virtual void SetVariant(IndexType, IVariantConstPtr) = 0;
    virtual void SetOptional(IndexType, IOptionalConstPtr) = 0;

    virtual void SetString(IndexType, std::string&&) = 0;
    virtual void SetStruct(IndexType, IStructPtr&&) = 0;
    virtual void SetTuple(IndexType, ITuplePtr&&) = 0;
    virtual void SetList(IndexType, IListPtr&&) = 0;
    virtual void SetDict(IndexType, IDictPtr&&) = 0;
    virtual void SetVariant(IndexType, IVariantPtr&&) = 0;
    virtual void SetOptional(IndexType, IOptionalPtr&&) = 0;

public:
    template <typename T>
    T GetValue(IndexType ind) const {
        using U = std::remove_cv_t<T>;

        if constexpr (std::is_same_v<U, bool>) {
            return GetBool(ind);
        } else if constexpr (std::is_same_v<U, int8_t>) {
            return GetInt8(ind);
        } else if constexpr (std::is_same_v<U, int16_t>) {
            return GetInt16(ind);
        } else if constexpr (std::is_same_v<U, int32_t>) {
            return GetInt32(ind);
        } else if constexpr (std::is_same_v<U, int64_t>) {
            return GetInt64(ind);
        } else if constexpr (std::is_same_v<U, uint8_t>) {
            return GetUInt8(ind);
        } else if constexpr (std::is_same_v<U, uint16_t>) {
            return GetUInt16(ind);
        } else if constexpr (std::is_same_v<U, uint32_t>) {
            return GetUInt32(ind);
        } else if constexpr (std::is_same_v<U, uint64_t>) {
            return GetUInt64(ind);
        } else if constexpr (std::is_same_v<U, float>) {
            return GetFloat(ind);
        } else if constexpr (std::is_same_v<U, double>) {
            return GetDouble(ind);
        } else if constexpr (std::is_same_v<U, std::string_view> || std::is_same_v<U, std::string>) {
            return U{GetString(ind)};
        } else if constexpr (std::is_same_v<U, IStructConstPtr>) {
            return GetStruct(ind);
        } else if constexpr (std::is_same_v<U, ITupleConstPtr>) {
            return GetTuple(ind);
        } else if constexpr (std::is_same_v<U, IListConstPtr>) {
            return GetList(ind);
        } else if constexpr (std::is_same_v<U, IDictConstPtr>) {
            return GetDict(ind);
        } else if constexpr (std::is_same_v<U, IVariantConstPtr>) {
            return GetVariant(ind);
        } else if constexpr (std::is_same_v<U, IOptionalConstPtr>) {
            return GetOptional(ind);
        } else if constexpr (std::is_same_v<U, IStructPtr>) {
            return GetStruct(ind)->CopyStruct();
        } else if constexpr (std::is_same_v<U, ITuplePtr>) {
            return GetTuple(ind)->CopyTuple();
        } else if constexpr (std::is_same_v<U, IListPtr>) {
            return GetList(ind)->CopyList();
        } else if constexpr (std::is_same_v<U, IDictPtr>) {
            return GetDict(ind)->CopyDict();
        } else if constexpr (std::is_same_v<U, IVariantPtr>) {
            return GetVariant(ind)->CopyVariant();
        } else if constexpr (std::is_same_v<U, IOptionalPtr>) {
            return GetOptional(ind)->CopyOptional();
        } else {
            static_assert(false, "Unsopported type");
        }
    }

    template <typename T>
    void SetValue(IndexType ind, T&& data) {
        using U = std::remove_cv_t<std::remove_reference_t<T>>;

        if constexpr (std::is_same_v<U, bool>) {
            return SetBool(ind, data);
        } else if constexpr (std::is_same_v<U, int8_t>) {
            return SetInt8(ind, data);
        } else if constexpr (std::is_same_v<U, int16_t>) {
            return SetInt16(ind, data);
        } else if constexpr (std::is_same_v<U, int32_t>) {
            return SetInt32(ind, data);
        } else if constexpr (std::is_same_v<U, int64_t>) {
            return SetInt64(ind, data);
        } else if constexpr (std::is_same_v<U, uint8_t>) {
            return SetUInt8(ind, data);
        } else if constexpr (std::is_same_v<U, uint16_t>) {
            return SetUInt16(ind, data);
        } else if constexpr (std::is_same_v<U, uint32_t>) {
            return SetUInt32(ind, data);
        } else if constexpr (std::is_same_v<U, uint64_t>) {
            return SetUInt64(ind, data);
        } else if constexpr (std::is_same_v<U, float>) {
            return SetFloat(ind, data);
        } else if constexpr (std::is_same_v<U, double>) {
            return SetDouble(ind, data);
        } else if constexpr (std::is_same_v<U, std::string> || 
                             std::is_same_v<U, std::string_view>) {
            return SetString(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, IStructPtr> || 
                             std::is_same_v<U, IStructConstPtr>) {
            return SetStruct(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, ITuplePtr> || 
                             std::is_same_v<U, ITupleConstPtr>) {
            return SetTuple(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, IListPtr> || 
                             std::is_same_v<U, IListConstPtr>) {
            return SetList(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, IDictPtr> || 
                             std::is_same_v<U, IDictConstPtr>) {
            return SetDict(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, IVariantPtr> || 
                             std::is_same_v<U, IVariantConstPtr>) {
            return SetVariant(ind, std::forward<T>(data));
        } else if constexpr (std::is_same_v<U, IOptionalPtr> || 
                             std::is_same_v<U, IOptionalConstPtr>) {
            return SetOptional(ind, std::forward<T>(data));
        } else {
            static_assert(false, "Unsopported type");
        }
    }
};

class IBaseStruct : virtual public IBaseIndexed<std::string_view> {
public:
    virtual IStructPtr CopyStruct() const = 0;
    virtual std::vector<std::string> FieldsNames() const = 0;
};

class IBaseTuple : virtual public IBaseIndexed<size_t>  {
public:
    virtual ITuplePtr CopyTuple() const = 0;
    virtual size_t FieldsCount() const = 0;
};

class IBaseRow : virtual public IBaseStruct, virtual public IBaseTuple {
public:
    virtual IRowPtr CopyRow() const = 0;

    using IBaseStruct::GetValue;
    using IBaseTuple::GetValue;
    using IBaseStruct::SetValue;
    using IBaseTuple::SetValue;

private:
    IStructPtr CopyStruct() const override = 0;
    ITuplePtr CopyTuple() const override = 0;
};

class IBaseList : virtual public IBaseIndexed<size_t>  {
public:
    virtual IListPtr CopyList() const = 0;

    virtual size_t Size() const = 0;
    virtual void Clear() = 0;
    virtual void PopBack() = 0;

    template <typename T>
    void PushBack(T&& value) {
        Extend();
        SetValue(Size() - 1, std::forward<T>(value));
    }

    virtual void Extend() = 0;  // Creates new element at back

    // TODO: Implement AsStdVector
};

class IBaseVariant : virtual public IBaseIndexed<size_t>  {
public:
    virtual IVariantPtr CopyVariant() const = 0;

    virtual size_t VariantsCount() const = 0;
    virtual size_t VariantNumber() const = 0;

    template <typename T>
    T GetValue() const {
        return IBaseIndexed::GetValue<T>(VariantNumber());
    }

    virtual void EmplaceVariant(size_t number) = 0;  // Creates new value at variant number

    // TODO: Implement AsStdVariant
};

class IBaseOptional : virtual public IBaseIndexed<bool>  {
public:
    virtual IOptionalPtr CopyOptional() const = 0;

    virtual bool HasValue() const = 0;
    virtual void ClearValue() = 0;

    template <typename T>
    T GetValue() const {
        return IBaseIndexed::GetValue<T>(true);
    }

    template <typename T>
    void SetValue(T&& value) {
        return IBaseIndexed::SetValue<T>(true, std::forward<T>(value));
    }

    virtual void EmplaceValue() = 0;  // Creates new value

    // TODO: Implement AsStdOptional
};

class IBaseDict : virtual public IBaseList {
public:
    virtual IDictPtr CopyDict() const = 0;

    template<typename TKey, typename TValue>
    std::map<TKey, TValue> AsMap() const {
        std::map<TKey, TValue> res;

        for (size_t i = 0; i < Size(); ++i) {
            auto kv = GetStruct(i);
            res[kv->GetValue<TKey>("key")] = kv->GetValue<TValue>("value");
        }

        return res;
    }

    template<typename TKey, typename TValue>
    std::unordered_map<TKey, TValue> AsUnorderedMap() const {
        std::unordered_map<TKey, TValue> res;

        for (size_t i = 0; i < Size(); ++i) {
            auto kv = GetStruct(i);
            res[kv->GetValue<TKey>("key")] = kv->GetValue<TValue>("value");
        }

        return res;
    }

    template<typename TKey, typename TValue>
    void FromMap(const std::map<TKey, TValue>& map) {
        Clear();

        Extend();
        auto kv = GetStruct(Size() - 1)->CopyStruct();
        PopBack();

        for (const auto& [key, value] : map) {
            kv->SetValue("key", TKey(key));
            kv->SetValue("value", TValue(value));
            PushBack<IStructConstPtr>(kv);
        }
    }

    template<typename TKey, typename TValue>
    void FromUnorderedMap(const std::unordered_map<TKey, TValue>& map) {
        Clear();

        Extend();
        auto kv = GetStruct(Size() - 1)->CopyStruct();
        PopBack();

        for (const auto& [key, value] : map) {
            kv->SetValue("key", TKey(key));
            kv->SetValue("value", TValue(value));
            PushBack<IStructConstPtr>(kv);
        }
    }

private:
    IListPtr CopyList() const override = 0;
};

}
