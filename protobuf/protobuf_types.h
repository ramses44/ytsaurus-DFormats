#pragma once

#include <string>
#include <memory>
#include <vector>
#include <optional>
#include <unordered_map>

#include <library/cpp/skiff/skiff_schema.h>
#include <library/cpp/type_info/type.h>
#include <util/generic/buffer.h>

#include <dformats/interface/types.h>
#include <dformats/common/indexed_proxy.h>
#include <dformats/protobuf/protobuf_schema.h>

using namespace google::protobuf;

namespace DFormats {

class TProtobufObject;
class TProtobufStruct;
class TProtobufTuple;
class TProtobufList;
class TProtobufDict;
class TProtobufVariant;
class TProtobufOptional;

class TProtobufObject : virtual public IBaseIndexed<size_t> {
public:
    TProtobufObject(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory);

    TProtobufObject(const TProtobufObject& rhs);
    TProtobufObject(TProtobufObject&& rhs);
    
    TProtobufObject& operator=(const TProtobufObject& rhs);
    TProtobufObject& operator=(TProtobufObject&& rhs);

    virtual ~TProtobufObject() = default;

    std::shared_ptr<Message> Release();
    const Message* RawMessage() const;
    Message* RawMessage();

protected:
    bool GetBool(size_t ind) const override;
    int8_t GetInt8(size_t ind) const override;
    int16_t GetInt16(size_t ind) const override;
    int32_t GetInt32(size_t ind) const override;
    int64_t GetInt64(size_t ind) const override;
    uint8_t GetUInt8(size_t ind) const override;
    uint16_t GetUInt16(size_t ind) const override;
    uint32_t GetUInt32(size_t ind) const override;
    uint64_t GetUInt64(size_t ind) const override;
    float GetFloat(size_t ind) const override;
    double GetDouble(size_t ind) const override;
    std::string_view GetString(size_t ind) const override;
    IStructConstPtr GetStruct(size_t ind) const override;
    ITupleConstPtr GetTuple(size_t ind) const override;
    IListConstPtr GetList(size_t ind) const override;
    IDictConstPtr GetDict(size_t ind) const override;
    IVariantConstPtr GetVariant(size_t ind) const override;
    IOptionalConstPtr GetOptional(size_t ind) const override;

    void SetBool(size_t ind, bool value) override;
    void SetInt8(size_t ind, int8_t value) override;
    void SetInt16(size_t ind, int16_t value) override;
    void SetInt32(size_t ind, int32_t value) override;
    void SetInt64(size_t ind, int64_t value) override;
    void SetUInt8(size_t ind, uint8_t value) override;
    void SetUInt16(size_t ind, uint16_t value) override;
    void SetUInt32(size_t ind, uint32_t value) override;
    void SetUInt64(size_t ind, uint64_t value) override;
    void SetFloat(size_t ind, float value) override;
    void SetDouble(size_t ind, double value) override;
    void SetString(size_t ind, std::string_view value) override;
    void SetStruct(size_t ind, IStructConstPtr value) override;
    void SetTuple(size_t ind, ITupleConstPtr value) override;
    void SetList(size_t ind, IListConstPtr value) override;
    void SetDict(size_t ind, IDictConstPtr value) override;
    void SetVariant(size_t ind, IVariantConstPtr value) override;
    void SetOptional(size_t ind, IOptionalConstPtr value) override;

    void SetString(size_t ind, std::string&& value) override;
    void SetStruct(size_t ind, IStructPtr&& value) override;
    void SetTuple(size_t ind, ITuplePtr&& value) override;
    void SetList(size_t ind, IListPtr&& value) override;
    void SetDict(size_t ind, IDictPtr&& value) override;
    void SetVariant(size_t ind, IVariantPtr&& value) override;
    void SetOptional(size_t ind, IOptionalPtr&& value) override;

protected:
    virtual bool IsRepeated() const;
    virtual const FieldDescriptor* GetFieldDescriptor(size_t ind) const;
    const Reflection* GetReflection() const;
    std::shared_ptr<Message> GetMessage() const;
    TString* GetScratchString(size_t) const;
    std::shared_ptr<MessageFactory> Factory() const;

private:
    std::shared_ptr<Message> Underlying_;
    std::shared_ptr<MessageFactory> Factory_;
    mutable std::vector<std::optional<TString>> ScratchStrings_;
};

class TProtobufStruct : virtual public TProtobufObject, virtual public IBaseStruct, virtual public IIndexedProxy<std::string_view, size_t> {
public:
    TProtobufStruct(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory);
    TProtobufStruct(const TProtobufStruct& rhs);
    TProtobufStruct(TProtobufStruct&& rhs);

    TProtobufStruct& operator=(const TProtobufStruct& rhs);
    TProtobufStruct& operator=(TProtobufStruct&& rhs);

    IStructPtr CopyStruct() const override;
    std::vector<std::string> FieldsNames() const override;

protected:
    size_t GetIndex(std::string_view name) const override;

private:
    mutable std::unordered_map<std::string, size_t> IndexesMap_;
};

class TProtobufVariant : virtual public TProtobufStruct, virtual public IBaseVariant {
public:
    TProtobufVariant(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory);
    TProtobufVariant(const TProtobufVariant& rhs);
    TProtobufVariant(TProtobufVariant&& rhs);

    TProtobufVariant& operator=(const TProtobufVariant& rhs);
    TProtobufVariant& operator=(TProtobufVariant&& rhs);

    IVariantPtr CopyVariant() const override;
    size_t VariantsCount() const override;
    size_t VariantNumber() const override;
    void EmplaceVariant(size_t number) override;
};

class TProtobufOptional : virtual public TProtobufObject, virtual public IBaseOptional, virtual public IIndexedProxy<bool, size_t> {
public:
    TProtobufOptional(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex);
    TProtobufOptional(const TProtobufOptional& rhs);
    TProtobufOptional(TProtobufOptional&& rhs);

    TProtobufOptional& operator=(const TProtobufOptional& rhs);
    TProtobufOptional& operator=(TProtobufOptional&& rhs);

    IOptionalPtr CopyOptional() const override;

    bool HasValue() const override;
    void ClearValue() override;
    void EmplaceValue() override;

protected:
    size_t GetIndex(bool) const override;

private:
    size_t FieldIndex_;
};

class TProtobufList : virtual public TProtobufObject, virtual public IBaseList {
public:
    TProtobufList(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex);
    TProtobufList(const TProtobufList& rhs);
    TProtobufList(TProtobufList&& rhs);

    TProtobufList& operator=(const TProtobufList& rhs);
    TProtobufList& operator=(TProtobufList&& rhs);

    IListPtr CopyList() const override;

    size_t Size() const override;
    void Clear() override;
    void PopBack() override;

    void Extend() override;

    bool IsRepeated() const override;
    const FieldDescriptor* GetFieldDescriptor(size_t ind) const override;

protected:
    inline size_t FieldIndex() const {
        return FieldIndex_;
    }
    inline void SetFieldIndex(size_t ind) {
        FieldIndex_ = ind;
    }

private:
    size_t FieldIndex_;
};

class TProtobufDict : virtual public TProtobufList, virtual public IBaseDict {
public:
    TProtobufDict(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex);
    TProtobufDict(const TProtobufDict& rhs);
    TProtobufDict(TProtobufDict&& rhs);

    TProtobufDict& operator=(const TProtobufDict& rhs);
    TProtobufDict& operator=(TProtobufDict&& rhs);

    inline IDictPtr CopyDict() const override {
        return std::make_shared<TProtobufDict>(*this);
    }

private:
    inline IListPtr CopyList() const override {
        return TProtobufList::CopyList();
    }
};

class TProtobufRow : virtual public TProtobufStruct, virtual public IBaseRow {
public:
    TProtobufRow(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory);
    TProtobufRow(const TProtobufRow& rhs);
    TProtobufRow(TProtobufRow&& rhs);

    TProtobufRow& operator=(const TProtobufRow& rhs);
    TProtobufRow& operator=(TProtobufRow&& rhs);

    inline IRowPtr CopyRow() const override {
        return std::make_shared<TProtobufRow>(*this);
    }

private:
    inline ITuplePtr CopyTuple() const override {
        return nullptr;
    }
    inline IStructPtr CopyStruct() const override {
        return TProtobufStruct::CopyStruct();
    }
    inline size_t FieldsCount() const override {
        return GetMessage()->GetDescriptor()->field_count();
    }
};

}