#include <library/cpp/skiff/skiff.h>

#include  <functional>

#include "skiff_types.h"

TString EwireTypeStr(NSkiff::EWireType type) {
    switch (type) {
    case NSkiff::EWireType::Nothing:
        return "Nothing";
    case NSkiff::EWireType::Int8:
        return "Int8";
    case NSkiff::EWireType::Uint8:
        return "Uint8";
    case NSkiff::EWireType::Int16:
        return "Int16";
    case NSkiff::EWireType::Uint16:
        return "Uint16";
    case NSkiff::EWireType::Int32:
        return "Int32";
    case NSkiff::EWireType::Uint32:
        return "Uint32";
    case NSkiff::EWireType::Int64:
        return "Int64";
    case NSkiff::EWireType::Uint64:
        return "Uint64";
    case NSkiff::EWireType::Int128:
        return "Int128";
    case NSkiff::EWireType::Uint128:
        return "Uint128";
    case NSkiff::EWireType::Int256:
        return "Int256";
    case NSkiff::EWireType::Uint256:
        return "Uint256";
    case NSkiff::EWireType::Boolean:
        return "Boolean";
    case NSkiff::EWireType::Double:
        return "Double";
    case NSkiff::EWireType::String32:
        return "String32";
    case NSkiff::EWireType::Yson32:
        return "Yson32";
    case NSkiff::EWireType::Variant8:
        return "Variant8";
    case NSkiff::EWireType::Variant16:
        return "Variant16";
    case NSkiff::EWireType::RepeatedVariant8:
        return "RepeatedVariant8";
    case NSkiff::EWireType::RepeatedVariant16:
        return "RepeatedVariant16";
    case NSkiff::EWireType::Tuple:
        return "Tuple";
    }
}

// TODO: Сделать возможность передать адрес char* для inplace сериализации (unsafe)
TString SkiffSerializeString(TStringView str) {
    TString res;

    uint32_t strSize = str.size();
    res.append(reinterpret_cast<char*>(&strSize), sizeof(strSize));
    res.append(str.data(), strSize);

    return res;
}

// TODO: Сделать возможность передать адрес char* для inplace десериализации (unsafe)
TString SkiffDeserializeString(const char* skiffStr) {
    return TString(skiffStr + sizeof(uint32_t), *reinterpret_cast<const uint32_t*>(skiffStr));
}

size_t AddField(const NSkiff::TSkiffSchemaPtr fieldSkiffSchema, TBuffer& dst) {
    auto offset = dst.Size();

    switch (fieldSkiffSchema->GetWireType()) {
    // Simple types
    case NSkiff::EWireType::Int8:
    case NSkiff::EWireType::Uint8:
    case NSkiff::EWireType::Boolean:
        dst.Advance(1);
        break;
    case NSkiff::EWireType::Int16:
    case NSkiff::EWireType::Uint16:
        dst.Advance(2);
        break;
    case NSkiff::EWireType::Int32:
    case NSkiff::EWireType::Uint32:
        dst.Advance(4);
    case NSkiff::EWireType::Int64:
    case NSkiff::EWireType::Uint64:
    case NSkiff::EWireType::Double:
        dst.Advance(8);
        break;
    case NSkiff::EWireType::Int128:
    case NSkiff::EWireType::Uint128:
        dst.Advance(16);
        break;
    case NSkiff::EWireType::Int256:
    case NSkiff::EWireType::Uint256:
        dst.Advance(32);
        break;
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32:
        dst.Append("\00\00\00\00", 4);
        break;

    // Complex types
    case NSkiff::EWireType::Tuple:
        for (auto childSkiffSchema : fieldSkiffSchema->GetChildren()) {
            AddField(childSkiffSchema, dst);
        }
        break;
    case NSkiff::EWireType::Variant8:
        dst.Append("\00", 1);
        AddField(fieldSkiffSchema->GetChildren()[0], dst);
        break;
    case NSkiff::EWireType::Variant16:
        dst.Append("\00\00", 2);
        AddField(fieldSkiffSchema->GetChildren()[0], dst);
        break;
    case NSkiff::EWireType::RepeatedVariant8: {
        auto rtag8 = TSkiffVariant::Terminal8Tag();
        dst.Append(reinterpret_cast<char*>(&rtag8), 1);
        break;
    }
    case NSkiff::EWireType::RepeatedVariant16: {
        auto rtag16 = TSkiffVariant::Terminal16Tag();
        dst.Append(reinterpret_cast<char*>(&rtag16), 2);
        break;
    }
    default:
        break;
    }

    return offset;
}

// TSkiffData

TSkiffData::TSkiffData() : Schema_(NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Nothing)) { }

TSkiffData::TSkiffData(NSkiff::TSkiffSchemaPtr schema) : Schema_(schema) { }

TSkiffData::TSkiffData(NSkiff::TSkiffSchemaPtr schema, TStringView skiffStr) 
  : Schema_(schema), Data_(skiffStr.data(), skiffStr.size()) { }

TSkiffData::TSkiffData(NSkiff::TSkiffSchemaPtr schema, TBuffer buf)
  : Schema_(schema), Data_(std::move(buf)) { }

TString TSkiffData::Serialize() const {
    // TODO: Сделать константный аналог Rebuild'а, который будет возвращать новый буффер 
    return TString(Data_.Begin(), Data_.End());
}

TString TSkiffData::Release() {
    TString res;
    Data_.AsString(res);
    return std::move(res);
}

void TSkiffData::Rebuild() {
}

void TSkiffData::ReplaceData(TStringView newData) {
    Data_.Assign(newData.data(), newData.size());
}

void TSkiffData::ApplyReplaceData(TSkiffData* object, TStringView newData) {
    object->ReplaceData(newData);
}

TBuffer& TSkiffData::GetData(TSkiffData* object) {
    return object->Data_;
}

// TSkiffVariant

uint8_t TSkiffVariant::Terminal8Tag() {
    return NSkiff::EndOfSequenceTag<uint8_t>();
}
uint16_t TSkiffVariant::Terminal16Tag() {
    return NSkiff::EndOfSequenceTag<uint16_t>();
}

TSkiffVariant::TSkiffVariant() : TSkiffData() { }

TSkiffVariant::TSkiffVariant(NSkiff::TSkiffSchemaPtr schema) : TSkiffData(schema) { 
    Y_ENSURE(schema->GetWireType() == NSkiff::EWireType::Variant8 ||
             schema->GetWireType() == NSkiff::EWireType::Variant16,
             "Bad Skiff schema provided. EWireType should be Variant8 or Variant16");

    ReserveBuffer(0);
    SetTag(TagSize() == 1 ? Terminal8Tag() : Terminal16Tag());
}

template <class T>
TSkiffVariant::TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, uint16_t tag, T data) 
  : TSkiffVariant(schema)
{
    SetData(tag, data);
}

TSkiffVariant::TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, TStringView skiffStr)
  : TSkiffData(schema, skiffStr) { }

TSkiffVariant::TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, TBuffer&& buf) 
  : TSkiffData(schema, std::move(buf)) { }

TSkiffVariant::TSkiffVariant(const TSkiffVariant& rhs) : TSkiffData(rhs.Schema_, rhs.Data_) { }

TSkiffVariant::TSkiffVariant(TSkiffVariant&& rhs) noexcept
  : TSkiffData(rhs.Schema_, std::move(rhs.Data_)) { }

TSkiffVariant& TSkiffVariant::operator=(const TSkiffVariant& rhs) {
    Schema_ = rhs.Schema_;
    Data_ = rhs.Data_;

    return *this;
}

TSkiffVariant& TSkiffVariant::operator=(TSkiffVariant&& rhs) noexcept {
    Schema_ = rhs.Schema_;
    Data_ = std::move(rhs.Data_);

    return *this;
}

uint16_t TSkiffVariant::GetTag() const {
    if (TagSize() == 1) {
        return *reinterpret_cast<uint8_t*>(Data_.data());
    } else {
        return *reinterpret_cast<uint16_t*>(Data_.data());
    }
}

bool TSkiffVariant::IsTerminal() const {
    return GetTag() == (TagSize() == 1 ? Terminal8Tag() : Terminal16Tag());
}

template <class T>
void TSkiffVariant::SetData(uint16_t tag, T data) {
    SetTag(tag);
    Data_.Append(dynamic_cast<char*>(&data), sizeof(data));
}

template <>
void TSkiffVariant::SetData<TStringView>(uint16_t tag, TStringView data) {
    SetTag(tag);
    auto skiffStr = SkiffSerializeString(data);
    Data_.Append(skiffStr.begin(), skiffStr.end());
}

template<>
void TSkiffVariant::SetData<TSkiffList>(uint16_t /* tag */, TSkiffList /* data */) {
    ythrow yexception() << "Not Implemented!";
}

template<>
void TSkiffVariant::SetData<TSkiffTuple>(uint16_t /* tag */, TSkiffTuple /* data */) {
    ythrow yexception() << "Not Implemented!";
}

void TSkiffVariant::SetTag(uint16_t tag) {
    Data_.Clear();
    Y_ENSURE(Data_.Capacity() >= TagSize());
    Data_.Append(reinterpret_cast<char*>(&tag), TagSize());
}

void TSkiffVariant::ReserveBuffer(size_t size) {
    Data_.Reserve(TagSize() + size);
}

size_t TSkiffVariant::TagSize() const {
    return Schema_->GetWireType() == NSkiff::EWireType::Variant8 ? 1 : 2;
}

// TSkiffTuple

TSkiffTuple::TSkiffTuple() : TSkiffData() { }

TSkiffTuple::TSkiffTuple(NSkiff::TSkiffSchemaPtr schema)
  : TSkiffData(schema)
  , Modificated_(schema->GetChildren().size(), nullptr)
{ 
    Y_ENSURE(schema->GetWireType() == NSkiff::EWireType::Tuple, 
             "Bad Skiff schema provided. EWireType should be Tuple");
}

TSkiffTuple::TSkiffTuple(const TSkiffTuple& rhs) {
    *this = rhs;
}

TSkiffTuple::TSkiffTuple(TSkiffTuple&& rhs) noexcept {
    *this = std::move(rhs);
}

TSkiffTuple::~TSkiffTuple() {
    for (auto* modField : Modificated_) {
        if (modField) delete modField;
    }
}

TSkiffTuple& TSkiffTuple::operator=(const TSkiffTuple& rhs) {
    Schema_ = rhs.Schema_;
    Data_ = rhs.Data_;
    FieldsDataOffsets_ = rhs.FieldsDataOffsets_;
    Modificated_ = rhs.Modificated_;

    return *this;
}

TSkiffTuple& TSkiffTuple::operator=(TSkiffTuple&& rhs) noexcept {
    Schema_ = std::move(rhs.Schema_);
    Data_ = std::move(rhs.Data_);
    FieldsDataOffsets_ = std::move(rhs.FieldsDataOffsets_);
    Modificated_ = std::move(rhs.Modificated_);

    return *this;
}

void TSkiffTuple::Rebuild() {
    bool needTotalRebuild = false;
    bool needAnyRebuild = false;
    for (size_t i = 0; i < Modificated_.size(); ++i) {
        if (Modificated_[i]) {
            needAnyRebuild = true;
            Modificated_[i]->Rebuild();
            // if (Modificated_[i]->Data_.Size() != SerializedFieldDataSize(i)) {
            if (TSkiffData::GetData(Modificated_[i]).Size() != SerializedFieldDataSize(i)) {
                needTotalRebuild = true;
            }
        }
    }

    if (!needTotalRebuild) {
        if (needAnyRebuild) {
            for (size_t i = 0; i < Modificated_.size(); ++i) {
                if (Modificated_[i]) {
                    std::memcpy(GetFieldDataPtr(i),
                                TSkiffData::GetData(Modificated_[i]).Data(),
                                TSkiffData::GetData(Modificated_[i]).Size());
                    delete Modificated_[i];
                    Modificated_[i] = nullptr;
                }
            }
        }
        return;
    }

    TBuffer newData;  // TODO: Сделать расчёт, сколько нужно резервировать памяти заранее

    for (size_t i = 0; i < Modificated_.size(); ++i) {
        auto newOffset = newData.Size(); 
        if (Modificated_[i]) {
            newData.Append(TSkiffData::GetData(Modificated_[i]).Data(),
                           TSkiffData::GetData(Modificated_[i]).Size());
            delete Modificated_[i];
            Modificated_[i] = nullptr;
        } else {
            newData.Append(GetFieldDataPtr(i), SerializedFieldDataSize(i));
        }
        FieldsDataOffsets_[i] = newOffset;
    }

    Data_ = std::move(newData);
}

template<class T>
T* TSkiffTuple::GetFieldDataPtr(size_t ind) {
    Y_ENSURE(ind < FieldsDataOffsets_.size(), "No field with such index.");
    return reinterpret_cast<T*>(Data_.Data() + FieldsDataOffsets_[ind]);
}

template<class T>
const T* TSkiffTuple::GetFieldDataPtr(size_t ind) const {
    Y_ENSURE(ind < FieldsDataOffsets_.size(), "No field with such index.");
    return reinterpret_cast<const T*>(Data_.Data() + FieldsDataOffsets_[ind]);
}

TStringView TSkiffTuple::GetRaw(size_t ind) const {
    if (Modificated_[ind]) {
        return TStringView(TSkiffData::GetData(Modificated_[ind]).Data(),
                           TSkiffData::GetData(Modificated_[ind]).Size());
    }
    return TStringView(GetFieldDataPtr(ind), SerializedFieldDataSize(ind));
}

void TSkiffTuple::SetRaw(size_t ind, TStringView data) {
    if (!Modificated_[ind] && SerializedFieldDataSize(ind) == data.size()) {
        std::memcpy(GetFieldDataPtr(ind), data.data(), data.size());
    } else {
        if (Modificated_[ind]) {
            try {
                // Modificated_[ind]->ReplaceData(data);
                TSkiffData::ApplyReplaceData(Modificated_[ind], data);
                return;
            } catch (...) {  // TODO: написать ReplaceData для всех потомков TSkiffData и убрать пересоздание
                delete Modificated_[ind];
            }
        }
        Modificated_[ind] = new TSkiffData(GetFieldSchema(ind), data);
    }
}

size_t TSkiffTuple::SerializedFieldDataSize(size_t ind) const {
  auto start = GetFieldDataPtr(ind);
  auto end = (ind + 1 < FieldsDataOffsets_.size() ? GetFieldDataPtr(ind + 1) : Data_.End());
  return end - start;
}

NSkiff::TSkiffSchemaPtr TSkiffTuple::GetFieldSchema(size_t ind) const {
  return Schema_->GetChildren().at(ind);
}

void TSkiffTuple::ReplaceData(TStringView /* newData */) {
    ythrow yexception() << "Not Implemented!";
}

std::unique_ptr<TSkiffTuple> TSkiffTuple::MakeNew(NSkiff::TSkiffSchemaPtr schema) {
    auto res = std::make_unique<TSkiffTuple>(schema);

    for (auto column : schema->GetChildren()) {
        res->FieldsDataOffsets_.push_back(AddField(column, res->Data_));
    }

    return std::move(res);
}