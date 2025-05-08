#include "skiff_types.h"

#include <library/cpp/skiff/skiff.h>
#include <dformats/common/util.h>

namespace DFormats {

template <typename... Args>
std::shared_ptr<TSkiffData> CreateSkiffData(const NTi::TTypePtr& type, Args&&... args) {
    switch (type->StripTags()->GetTypeName()) {
    case NTi::ETypeName::Struct:
        return std::make_shared<TSkiffStruct>(type, std::forward<Args>(args)...);
        break;
    case NTi::ETypeName::Tuple:
        return std::make_shared<TSkiffTuple>(type, std::forward<Args>(args)...);
        break;
    case NTi::ETypeName::List:
        return std::make_shared<TSkiffList>(type, std::forward<Args>(args)...);
        break;
    case NTi::ETypeName::Dict:
        return std::make_shared<TSkiffDict>(type, std::forward<Args>(args)...);
        break;
    case NTi::ETypeName::Variant:
        return std::make_shared<TSkiffVariant>(type, std::forward<Args>(args)...);
        break;
    case NTi::ETypeName::Optional:
        return std::make_shared<TSkiffOptional>(type, std::forward<Args>(args)...);
        break;
    default:
        return std::make_shared<TSkiffData>(type, std::forward<Args>(args)...);
        break;
    }
}

std::string SkiffSerializeString(std::string_view str) {
    std::string res;

    uint32_t strSize = str.size();
    res.append(reinterpret_cast<char*>(&strSize), sizeof(strSize));
    res.append(str.data(), strSize);

    return res;
}

std::string_view SkiffDeserializeString(const char* skiffStr) {
    return {skiffStr + 4, *reinterpret_cast<const uint32_t*>(skiffStr)};
}

size_t AddField(const NSkiff::TSkiffSchemaPtr& fieldSkiffSchema, TBuffer& dst) {
    auto offset = dst.Size();

    switch (fieldSkiffSchema->GetWireType()) {
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32:
        dst.Append("\00\00\00\00", 4);
        break;
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
        dst.Advance(SkiffSchemaStaticSize(fieldSkiffSchema));
        break;
    }

    return offset;
}

size_t SkiffDataSize(const NSkiff::TSkiffSchemaPtr& schema, const char* serialization) {
    switch (schema->GetWireType()) {
    case NSkiff::EWireType::String32:
    case NSkiff::EWireType::Yson32:
        return *reinterpret_cast<const uint32_t*>(serialization) + 4;
    case NSkiff::EWireType::Tuple:
    {
        size_t res = 0;
        for (const auto& childSchema : schema->GetChildren()) {
            res += SkiffDataSize(childSchema, serialization + res);
        }
        return res;
    }
    case NSkiff::EWireType::Variant8:
    {
        auto tag = *reinterpret_cast<const uint8_t*>(serialization);
        return SkiffDataSize(schema->GetChildren()[tag], serialization + 1) + 1;
    }
    case NSkiff::EWireType::Variant16:
    {
        auto tag = *reinterpret_cast<const uint16_t*>(serialization);
        return SkiffDataSize(schema->GetChildren()[tag], serialization + 2) + 2;
    }
    case NSkiff::EWireType::RepeatedVariant8:
    {
        size_t res = 0;

        uint8_t tag;
        while ((tag = *reinterpret_cast<const uint8_t*>(serialization + res)) != 
               NSkiff::EndOfSequenceTag<uint8_t>()) {
            
            res += SkiffDataSize(schema->GetChildren()[tag], serialization + res + 1) + 1;
        }

        return res + 1;
    }
    case NSkiff::EWireType::RepeatedVariant16:
    {
        size_t res = 0;

        uint16_t tag;
        while ((tag = *reinterpret_cast<const uint16_t*>(serialization + res)) != 
               NSkiff::EndOfSequenceTag<uint16_t>()) {
            
            res += SkiffDataSize(schema->GetChildren()[tag], serialization + res + 2) + 2;
        }

        return res + 2;
    }
    default:
        return SkiffSchemaStaticSize(schema);
    }
}

std::vector<ptrdiff_t> CalculateOffsets(const NSkiff::TSkiffSchemaList& schemas, std::string_view buf) {
    std::vector<ptrdiff_t> res { 0 };
    res.reserve(schemas.size() + 1);

    size_t currentTotalOffset = 0;

    for (const auto& childSchema : schemas) {
        currentTotalOffset += SkiffDataSize(childSchema, buf.data() + currentTotalOffset);
        res.emplace_back(currentTotalOffset);
    }

    res.pop_back();

    return std::move(res);
}

// TSkiffData

TSkiffData::TSkiffData(NTi::TTypePtr schema) : Schema_(schema) {
    AddField(SkiffSchemaFromTypeV3(Schema_), Data_);
}

TSkiffData::TSkiffData(NTi::TTypePtr schema, TBuffer&& buf)
  : Schema_(std::move(schema)), Data_(std::move(buf)) { }

TSkiffData::TSkiffData(const TSkiffData& rhs) : Schema_(rhs.Schema_), Data_(rhs.Data_) { }

TSkiffData::TSkiffData(TSkiffData&& rhs)
  : Schema_(std::move(rhs.Schema_)), Data_(std::move(rhs.Data_)) { }

TSkiffData& TSkiffData::operator=(const TSkiffData& rhs) {
    Schema_ = rhs.Schema_;
    Data_ = rhs.Data_;

    return *this;
}

TSkiffData& TSkiffData::operator=(TSkiffData&& rhs) {
    Schema_ = std::move(rhs.Schema_);
    Data_ = std::move(rhs.Data_);

    return *this;
}

size_t TSkiffData::Rebuild() {
    SoftRebuild();

    if (NeedRebuild()) {
        HardRebuild();
    }

    return Buffer().Size();
}

TBuffer TSkiffData::Serialize() & {
    Rebuild();
    return Data_;
}

TBuffer TSkiffData::Serialize() const & {
    return SerializeImpl();
}

TBuffer TSkiffData::Serialize() && {
    Rebuild();
    return std::move(Data_);
}

TBuffer TSkiffData::Serialize() const && {
    return SerializeImpl();
}

// TSkiffVariant

uint8_t TSkiffVariant::Terminal8Tag() {
    return NSkiff::EndOfSequenceTag<uint8_t>();
}
uint16_t TSkiffVariant::Terminal16Tag() {
    return NSkiff::EndOfSequenceTag<uint16_t>();
}

TSkiffVariant::TSkiffVariant(NTi::TTypePtr schema) : TSkiffData(schema) { }

TSkiffVariant::TSkiffVariant(NTi::TTypePtr schema, TBuffer&& buf) 
  : TSkiffData(schema, std::move(buf)) { } 

template <class T>
TSkiffVariant::TSkiffVariant(NTi::TTypePtr schema, uint16_t tag, T&& data) : TSkiffVariant(schema) {
    SetValue(tag, std::forward<T>(data));
}

TSkiffVariant::TSkiffVariant(const TSkiffVariant& rhs)
  : TSkiffData(rhs.GetSchema(), rhs.Serialize()) { }

TSkiffVariant::TSkiffVariant(TSkiffVariant&& rhs)
  : TSkiffData(rhs.GetSchema(), std::move(rhs.Buffer()))
  , ObjectiveValue_(std::move(rhs.ObjectiveValue_)) { }

TSkiffVariant& TSkiffVariant::operator=(const TSkiffVariant& rhs) {
    TSkiffData::operator=(TSkiffData(rhs.GetSchema(), rhs.Serialize()));
    ObjectiveValue_.reset();

    return *this;
}

TSkiffVariant& TSkiffVariant::operator=(TSkiffVariant&& rhs) {
    ObjectiveValue_ = std::move(rhs.ObjectiveValue_);
    TSkiffData::operator=(std::move(rhs));

    return *this;
}

IVariantPtr TSkiffVariant::CopyVariant() const {
    return std::make_shared<TSkiffVariant>(*this);
}

size_t TSkiffVariant::VariantNumber() const {
    if (TagSize() == 1) {
        return *reinterpret_cast<const uint8_t*>(Buffer().data());
    } else {
        return *reinterpret_cast<const uint16_t*>(Buffer().data());
    }
}

size_t TSkiffVariant::VariantsCount() const {
    auto variantSchema = GetSchema()->StripTags()->AsVariant();

    if (variantSchema->IsVariantOverTuple()) {
        return variantSchema->GetUnderlyingType()->AsTuple()->GetElements().size();
    } else {
        return variantSchema->GetUnderlyingType()->AsStruct()->GetMembers().size();
    }
}

bool TSkiffVariant::IsTerminal() const {
    return TagSize() == 1 && VariantNumber() == Terminal8Tag() || 
           TagSize() == 2 && VariantNumber() == Terminal16Tag();
}

void TSkiffVariant::EmplaceVariant(size_t number) {
    auto tagSize = TagSize();

    Y_ENSURE(number < (1 << (tagSize * 8)));

    Buffer().Clear();
    Buffer().Append(reinterpret_cast<const char*>(&number), tagSize);
    AddField(SkiffSchemaFromTypeV3(GetChildType(number)), Buffer());
}

size_t TSkiffVariant::TagSize() const {
    return VariantsCount() < Terminal8Tag() ? 1 : 2;
}

const char* TSkiffVariant::GetRawDataPtr(size_t ind) const {
    Y_ENSURE(ind == VariantNumber());
    return ObjectiveValue_ ? TSkiffData::GetBuffer(*ObjectiveValue_).Data() : Buffer().Data() + TagSize();
}

char* TSkiffVariant::GetRawDataPtr(size_t ind) {
    if (ObjectiveValue_) {
        return TSkiffData::GetBuffer(*ObjectiveValue_).Data();
    }

    if (VariantNumber() != ind) {
        EmplaceVariant(ind);
    }

    return Buffer().Data() + TagSize();
}

TSkiffDataConstPtr TSkiffVariant::GetSkiffDataPtr(size_t ind) const {
    if (ObjectiveValue_) {
        return ObjectiveValue_;
    }

    Y_ENSURE(ind == VariantNumber());

    auto tagSize = TagSize();
    TBuffer data(Buffer().Begin() + tagSize, Buffer().Size() - tagSize);

    return ObjectiveValue_ = CreateSkiffData(GetChildType(ind), std::move(data));
}

TSkiffDataPtr& TSkiffVariant::GetSkiffDataPtr(size_t ind) {
    if (ObjectiveValue_) {
        return ObjectiveValue_;
    }

    if (VariantNumber() != ind) {
        EmplaceVariant(ind);
    }

    auto tagSize = TagSize();

    return ObjectiveValue_ = CreateSkiffData(GetChildType(ind),
        TBuffer(Buffer().Data() + tagSize, Buffer().Size() - tagSize));
}

NTi::TTypePtr TSkiffVariant::GetChildType(size_t ind) const {
    auto variantSchema = GetSchema()->StripTags()->AsVariant();

    if (variantSchema->IsVariantOverTuple()) {
        return variantSchema->GetUnderlyingType()->AsTuple()->GetElements()[ind].GetType();
    } else {
        return variantSchema->GetUnderlyingType()->AsStruct()->GetMembers()[ind].GetType();
    }
}

bool TSkiffVariant::NeedRebuild() const {
    return static_cast<bool>(ObjectiveValue_);
}

TBuffer TSkiffVariant::SerializeImpl() const {
    if (!ObjectiveValue_) {
        return Buffer();
    }

    TBuffer res;
    TBuffer data = ObjectiveValue_->Serialize();

    res.Append(Buffer().Data(), TagSize());
    res.Append(data.Data(), data.Size());

    return res;
}

void TSkiffVariant::SoftRebuild() {
    HardRebuild();
}

void TSkiffVariant::HardRebuild() {
    if (!ObjectiveValue_) {
        return;
    }

    TBuffer data = std::move(*ObjectiveValue_).Serialize();
    ObjectiveValue_ = nullptr;

    Buffer().Resize(TagSize());
    Buffer().Append(data.Data(), data.Size());
}

// TSkiffOptional

TSkiffOptional::TSkiffOptional(NTi::TTypePtr schema) : TSkiffVariant(std::move(schema)) { }

TSkiffOptional::TSkiffOptional(NTi::TTypePtr schema, TBuffer&& buf)
  : TSkiffVariant(std::move(schema), std::move(buf)) { }

TSkiffOptional::TSkiffOptional(const TSkiffOptional& rhs)
  : TSkiffVariant(rhs) { }

TSkiffOptional::TSkiffOptional(TSkiffOptional&& rhs)
  : TSkiffVariant(std::move(rhs)) { }

template <class T>
TSkiffOptional::TSkiffOptional(NTi::TTypePtr schema, T&& data)
  : TSkiffVariant(std::move(schema), 1, std::forward(data)) { }

TSkiffOptional& TSkiffOptional::operator=(const TSkiffOptional& rhs) {
    TSkiffVariant::operator=(rhs);
    return *this;
}

TSkiffOptional& TSkiffOptional::operator=(TSkiffOptional&& rhs) {
    TSkiffVariant::operator=(std::move(rhs));
    return *this;
}

IOptionalPtr TSkiffOptional::CopyOptional() const {
    return std::make_shared<TSkiffOptional>(*this);
}

size_t TSkiffOptional::VariantsCount() const {
    return 2;
}

bool TSkiffOptional::HasValue() const {
    return VariantNumber();
}

void TSkiffOptional::ClearValue() {
    EmplaceVariant(0);
    ObjectiveValue() = nullptr;
}

void TSkiffOptional::EmplaceValue() {
    EmplaceVariant(1);
}


const char* TSkiffOptional::GetRawDataPtr(bool ind) const {
    return GetRawDataPtr(static_cast<size_t>(ind));
}

char* TSkiffOptional::GetRawDataPtr(bool ind) {
    return GetRawDataPtr(static_cast<size_t>(ind));
}

TSkiffDataConstPtr TSkiffOptional::GetSkiffDataPtr(bool ind) const {
    return GetSkiffDataPtr(static_cast<size_t>(ind));
}

TSkiffDataPtr& TSkiffOptional::GetSkiffDataPtr(bool ind) {
    return GetSkiffDataPtr(static_cast<size_t>(ind));
}

NTi::TTypePtr TSkiffOptional::GetChildType(bool ind) const {
    return ind ? GetSchema()->StripTags()->AsOptional()->GetItemType() : NTi::Null();
}

NTi::TTypePtr TSkiffOptional::GetChildType(size_t ind) const {
    return GetChildType(static_cast<bool>(ind));
}

// TSkiffList

TSkiffList::TSkiffList(NTi::TTypePtr schema) : TSkiffData(std::move(schema)) { }

TSkiffList::TSkiffList(NTi::TTypePtr schema, TBuffer&& buf)
  : TSkiffData(std::move(schema), std::move(buf)) {

    auto skiffSchema = SkiffSchemaFromTypeV3(GetSchema());
    auto elemSchema = skiffSchema->GetChildren()[0];

    ElementsOffsets_.push_back(0);

    while (*reinterpret_cast<const uint8_t*>(Buffer().Data() + ElementsOffsets_.back()) !=
           TSkiffVariant::Terminal8Tag()) {

        ElementsOffsets_.push_back(ElementsOffsets_.back() + 
            SkiffDataSize(elemSchema, Buffer().Data() + ElementsOffsets_.back() + 1) + 1);
    }

    ObjectiveValues_.assign(ElementsOffsets_.size() - 1, nullptr);
}

TSkiffList::TSkiffList(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> elementsOffsets)
  : TSkiffData(std::move(schema), std::move(buf))
  , ElementsOffsets_(std::move(elementsOffsets)) { }

TSkiffList::TSkiffList(const TSkiffList& rhs) : TSkiffData(rhs.GetSchema(), {}) {
    *this = rhs;
}

TSkiffList::TSkiffList(TSkiffList&& rhs) : TSkiffData(rhs.GetSchema(), {}) {
    *this = std::move(rhs);
}

TSkiffList& TSkiffList::operator=(const TSkiffList& rhs) {
    TSkiffData::operator=(TSkiffData(rhs.GetSchema(), rhs.Serialize()));

    auto skiffSchema = SkiffSchemaFromTypeV3(GetSchema());
    auto elemSchema = skiffSchema->GetChildren()[0];

    ElementsOffsets_.push_back(0);

    while (*reinterpret_cast<const uint8_t*>(Buffer().Data() + ElementsOffsets_.back()) !=
           TSkiffVariant::Terminal8Tag()) {

        ElementsOffsets_.push_back(ElementsOffsets_.back() + 
            SkiffDataSize(elemSchema, Buffer().Data() + ElementsOffsets_.back() + 1) + 1);
    }

    ObjectiveValues_.assign(ElementsOffsets_.size() - 1, nullptr);

    return *this;
}

TSkiffList& TSkiffList::operator=(TSkiffList&& rhs) {
    ElementsOffsets_ = std::move(rhs.ElementsOffsets_);
    ObjectiveValues_ = std::move(rhs.ObjectiveValues_);
    TSkiffData::operator=(std::move(rhs));

    return *this;
}

IListPtr TSkiffList::CopyList() const {
    return std::make_shared<TSkiffList>(*this);
}

size_t TSkiffList::Size() const {
    return ObjectiveValues_.size();
}

void TSkiffList::Clear() {
    ElementsOffsets_ = { 0 };
    ObjectiveValues_.clear();
    
    Buffer().Clear();
    Buffer().Append(TSkiffVariant::Terminal8Tag());
}

void TSkiffList::PopBack() {
    ElementsOffsets_.pop_back();
    
    Buffer().Resize(ElementsOffsets_.back());
    Buffer().Append(TSkiffVariant::Terminal8Tag());

    ObjectiveValues_.pop_back();
}

void TSkiffList::Extend() {
    ObjectiveValues_.emplace_back();

    *reinterpret_cast<uint8_t*>(Buffer().End() - 1) = 0;
    AddField(SkiffSchemaFromTypeV3(GetChildType(0)), Buffer());

    ElementsOffsets_.push_back(Buffer().Size());
    Buffer().Append(TSkiffVariant::Terminal8Tag());
}

const char* TSkiffList::GetRawDataPtr(size_t ind) const {
    return ObjectiveValues_[ind] ? TSkiffData::GetBuffer(*ObjectiveValues_[ind]).Data()
                                 : Buffer().Data() + ElementsOffsets_[ind] + 1;
}

char* TSkiffList::GetRawDataPtr(size_t ind) {
    return ObjectiveValues_[ind] ? TSkiffData::GetBuffer(*ObjectiveValues_[ind]).Data()
                                 : Buffer().Data() + ElementsOffsets_[ind] + 1;
}

TSkiffDataConstPtr TSkiffList::GetSkiffDataPtr(size_t ind) const {
    if (ObjectiveValues_[ind]) {
        return ObjectiveValues_[ind];
    }

    TBuffer data(GetRawDataPtr(ind), ElementsOffsets_[ind + 1] - ElementsOffsets_[ind] - 1);

    return ObjectiveValues_[ind] = CreateSkiffData(GetChildType(ind), std::move(data));
}

TSkiffDataPtr& TSkiffList::GetSkiffDataPtr(size_t ind) {
    if (ObjectiveValues_[ind]) {
        return ObjectiveValues_[ind];
    }

    TBuffer data(GetRawDataPtr(ind), ElementsOffsets_[ind + 1] - ElementsOffsets_[ind] - 1);

    return ObjectiveValues_[ind] = CreateSkiffData(GetChildType(ind), std::move(data));
}

NTi::TTypePtr TSkiffList::GetChildType(size_t /* ind */) const {
    return GetSchema()->StripTags()->AsList()->GetItemType();
}

bool TSkiffList::NeedRebuild() const {
    return std::any_of(ObjectiveValues_.begin(), ObjectiveValues_.end(),
                       [](const TSkiffDataPtr& ptr) { return static_cast<bool>(ptr); });
}

TBuffer TSkiffList::SerializeImpl() const {
    if (!NeedRebuild()) {
        return Buffer();
    }

    TBuffer res;
    
    for (size_t i = 0; i < Size(); ++i) {
        res.Append(0);

        if (ObjectiveValues_[i]) {
            TBuffer data = ObjectiveValues_[i]->Serialize();
            res.Append(data.Data(), data.Size());
        } else {
            res.Append(GetRawDataPtr(i), ElementsOffsets_[i + 1] - ElementsOffsets_[i] - 1);
        }
    }

    res.Append(TSkiffVariant::Terminal8Tag());

    return res;
}

void TSkiffList::SoftRebuild() {
    for (size_t i = 0; i < Size(); ++i) {
        if (ObjectiveValues_[i]) {
            size_t currentSerializationSize = ElementsOffsets_[i + 1] - ElementsOffsets_[i] - 1;

            if (ObjectiveValues_[i]->Rebuild() == currentSerializationSize) {
                TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
                std::memcpy(Buffer().Data() + ElementsOffsets_[i] + 1, data.Data(), data.Size());

                ObjectiveValues_[i] = nullptr;
            } else if (i == Size() - 1) {
                Buffer().Resize(ElementsOffsets_[i] + 1);

                TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
                Buffer().Append(data.Data(), data.Size());

                ObjectiveValues_[i] = nullptr;
                ElementsOffsets_.back() = Buffer().Size();
                Buffer().Append(TSkiffVariant::Terminal8Tag());
            }
        } 
    }
}

void TSkiffList::HardRebuild() {
    TBuffer res;
    std::vector<ptrdiff_t> newOffsets = { 0 };
    newOffsets.reserve(ElementsOffsets_.size());

    for (size_t i = 0; i < Size(); ++i) {
        res.Append(0);

        if (ObjectiveValues_[i]) {
            TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
            res.Append(data.Data(), data.Size());

            ObjectiveValues_[i] = nullptr;
        } else {
            res.Append(GetRawDataPtr(i),  ElementsOffsets_[i + 1] - ElementsOffsets_[i] - 1);
        }

        newOffsets.push_back(res.Size());
    }

    res.Append(TSkiffVariant::Terminal8Tag());

    Buffer() = std::move(res);
    ElementsOffsets_ = std::move(newOffsets);
}

// TSkiffTuple

TSkiffTuple::TSkiffTuple(NTi::TTypePtr schema) : TSkiffData(schema, {}) {
    auto skiffSchema = SkiffSchemaFromTypeV3(schema);
    const auto& children = skiffSchema->GetChildren();

    FieldsDataOffsets_.reserve(children.size());
    ObjectiveValues_.assign(children.size(), nullptr);

    for (const auto& child : children) {
        FieldsDataOffsets_.emplace_back(AddField(child, Buffer()));
    }
}

TSkiffTuple::TSkiffTuple(NTi::TTypePtr schema, TBuffer&& buf)
  : TSkiffData(std::move(schema), std::move(buf)) {

    auto skiffSchema = SkiffSchemaFromTypeV3(GetSchema());
    const auto& children = skiffSchema->GetChildren();

    FieldsDataOffsets_ = CalculateOffsets(children, {Buffer().Data(), Buffer().Size()});
    ObjectiveValues_.assign(children.size(), nullptr);
}

TSkiffTuple::TSkiffTuple(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets)
  : TSkiffData(std::move(schema), std::move(buf))
  , FieldsDataOffsets_(std::move(fieldsOffsets))
  , ObjectiveValues_(FieldsDataOffsets_.size()) {
}

TSkiffTuple::TSkiffTuple(const TSkiffTuple& rhs) : TSkiffData(rhs.GetSchema(), rhs.Serialize()) {
    auto skiffSchema = SkiffSchemaFromTypeV3(GetSchema());
    const auto& children = skiffSchema->GetChildren();

    FieldsDataOffsets_ = CalculateOffsets(children, {Buffer().Data(), Buffer().Size()});
    ObjectiveValues_.assign(children.size(), nullptr);
}

TSkiffTuple::TSkiffTuple(TSkiffTuple&& rhs)
  : TSkiffData(rhs.GetSchema(), std::move(rhs.Buffer()))
  , FieldsDataOffsets_(std::move(rhs.FieldsDataOffsets_))
  , ObjectiveValues_(std::move(rhs.ObjectiveValues_)) { }

TSkiffTuple& TSkiffTuple::operator=(const TSkiffTuple& rhs) {
    TSkiffData::operator=(TSkiffData(rhs.GetSchema(), rhs.Serialize()));

    auto skiffSchema = SkiffSchemaFromTypeV3(GetSchema());
    const auto& children = skiffSchema->GetChildren();

    FieldsDataOffsets_ = CalculateOffsets(children, {Buffer().Data(), Buffer().Size()});
    ObjectiveValues_.assign(children.size(), nullptr);

    return *this;
}

TSkiffTuple& TSkiffTuple::operator=(TSkiffTuple&& rhs) {
    FieldsDataOffsets_ = std::move(rhs.FieldsDataOffsets_);
    ObjectiveValues_ = std::move(rhs.ObjectiveValues_);

    TSkiffData::operator=(std::move(rhs));

    return *this;
}

ITuplePtr TSkiffTuple::CopyTuple() const {
    return std::make_shared<TSkiffTuple>(*this);
}

size_t TSkiffTuple::FieldsCount() const {
    return ObjectiveValues_.size();
}

const char* TSkiffTuple::GetRawDataPtr(size_t ind) const {
    return ObjectiveValues_[ind] ? TSkiffData::GetBuffer(*ObjectiveValues_[ind]).Data()
                                 : Buffer().Data() + FieldsDataOffsets_[ind];
}

char* TSkiffTuple::GetRawDataPtr(size_t ind) {
    return ObjectiveValues_[ind] ? TSkiffData::GetBuffer(*ObjectiveValues_[ind]).Data()
                                 : Buffer().Data() + FieldsDataOffsets_[ind];
}

TSkiffDataConstPtr TSkiffTuple::GetSkiffDataPtr(size_t ind) const {
    if (ObjectiveValues_[ind]) {
        return ObjectiveValues_[ind];
    }

    TBuffer data(GetRawDataPtr(ind),
        (ind < FieldsCount() - 1 ? FieldsDataOffsets_[ind + 1] : Buffer().Size()) - 
        FieldsDataOffsets_[ind]);

    return ObjectiveValues_[ind] = CreateSkiffData(GetChildType(ind), std::move(data));
}

TSkiffDataPtr& TSkiffTuple::GetSkiffDataPtr(size_t ind) {
    if (ObjectiveValues_[ind]) {
        return ObjectiveValues_[ind];
    }

    TBuffer data(GetRawDataPtr(ind),
        (ind < FieldsCount() - 1 ? FieldsDataOffsets_[ind + 1] : Buffer().Size()) - 
        FieldsDataOffsets_[ind]);

    return ObjectiveValues_[ind] = CreateSkiffData(GetChildType(ind), std::move(data));
}

NTi::TTypePtr TSkiffTuple::GetChildType(size_t ind) const {
    return GetSchema()->StripTags()->AsTuple()->GetElements()[ind].GetType();
}

bool TSkiffTuple::NeedRebuild() const {
    return std::any_of(ObjectiveValues_.begin(), ObjectiveValues_.end(),
                       [](const TSkiffDataPtr& ptr) { return static_cast<bool>(ptr); });
}

TBuffer TSkiffTuple::SerializeImpl() const {
    if (!NeedRebuild()) {
        return Buffer();
    }

    TBuffer res;
    
    for (size_t i = 0; i < FieldsCount(); ++i) {
        if (ObjectiveValues_[i]) {
            TBuffer data = ObjectiveValues_[i]->Serialize();
            res.Append(data.Data(), data.Size());
        } else {
            res.Append(GetRawDataPtr(i), (i < FieldsCount() - 1 ? FieldsDataOffsets_[i + 1] 
                                          : Buffer().Size()) - FieldsDataOffsets_[i]);
        }
    }

    return res;
}

void TSkiffTuple::SoftRebuild() {
    for (size_t i = 0; i < FieldsCount(); ++i) {
        if (ObjectiveValues_[i]) {
            size_t currentSerializationSize = (i < FieldsCount() - 1 ? FieldsDataOffsets_[i + 1] 
                                               : Buffer().Size()) - FieldsDataOffsets_[i];

            if (ObjectiveValues_[i]->Rebuild() == currentSerializationSize) {
                TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
                std::memcpy(Buffer().Data() + FieldsDataOffsets_[i], data.Data(), data.Size());

                ObjectiveValues_[i] = nullptr;
            } else if (i == FieldsCount() - 1) {
                Buffer().Resize(FieldsDataOffsets_[i]);

                TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
                Buffer().Append(data.Data(), data.Size());

                ObjectiveValues_[i] = nullptr;
            }
        } 
    }
}

void TSkiffTuple::HardRebuild() {
    TBuffer res;
    std::vector<ptrdiff_t> newOffsets = { 0 };
    newOffsets.reserve(FieldsDataOffsets_.size());

    for (size_t i = 0; i < FieldsCount(); ++i) {
        if (ObjectiveValues_[i]) {
            TBuffer data = std::move(*ObjectiveValues_[i]).Serialize();
            res.Append(data.Data(), data.Size());

            ObjectiveValues_[i] = nullptr;
        } else {
            res.Append(GetRawDataPtr(i),  (i < FieldsCount() - 1 ? FieldsDataOffsets_[i + 1] 
                                           : Buffer().Size()) - FieldsDataOffsets_[i]);
        }

        newOffsets.push_back(res.Size());
    }

    newOffsets.pop_back();

    Buffer() = std::move(res);
    FieldsDataOffsets_ = std::move(newOffsets);
}

// TSkiffStruct

TSkiffStruct::TIndexesMap TSkiffStruct::BuildIndexesMap(NTi::TStructTypePtr type) {
    TIndexesMap res;

    for (const auto& field : type->GetMembers()) {
        res[field.GetName()] = res.size();
    }

    return std::move(res);
}

TSkiffStruct::TSkiffStruct(NTi::TTypePtr schema)
  : TSkiffTuple(std::move(schema))
  , IndexesMap_(BuildIndexesMap(GetSchema()->StripTags()->AsStruct())) { }

TSkiffStruct::TSkiffStruct(NTi::TTypePtr schema, TBuffer&& buf)
  : TSkiffTuple(std::move(schema), std::move(buf))
  , IndexesMap_(BuildIndexesMap(GetSchema()->StripTags()->AsStruct())) { }


TSkiffStruct::TSkiffStruct(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets)
  : TSkiffTuple(std::move(schema), std::move(buf), std::move(fieldsOffsets))
  , IndexesMap_(BuildIndexesMap(GetSchema()->StripTags()->AsStruct())) { }

TSkiffStruct::TSkiffStruct(const TSkiffStruct& rhs)
  : TSkiffTuple(rhs)
  , IndexesMap_(rhs.IndexesMap_) { }

TSkiffStruct::TSkiffStruct(TSkiffStruct&& rhs)
  : TSkiffTuple(std::move(rhs))
  , IndexesMap_(std::move(rhs.IndexesMap_)) { }

TSkiffStruct& TSkiffStruct::operator=(const TSkiffStruct& rhs) {
    IndexesMap_ = rhs.IndexesMap_;
    TSkiffTuple::operator=(rhs);

    return *this;
}

TSkiffStruct& TSkiffStruct::operator=(TSkiffStruct&& rhs) {
    IndexesMap_ = std::move(rhs.IndexesMap_);
    TSkiffTuple::operator=(std::move(rhs));

    return *this;
}

IStructPtr TSkiffStruct::CopyStruct() const {
    return std::make_shared<TSkiffStruct>(*this);
}

std::vector<std::string> TSkiffStruct::FieldsNames() const {
    std::vector<std::string> res;
    res.reserve(FieldsCount());

    for (const auto& field : GetSchema()->StripTags()->AsStruct()->GetMembers()) {
        res.emplace_back(field.GetName());
    }

    return res;
}

const char* TSkiffStruct::GetRawDataPtr(std::string_view ind) const {
    return GetRawDataPtr(IndexesMap_.at(ind));
}

char* TSkiffStruct::GetRawDataPtr(std::string_view ind) {
    return GetRawDataPtr(IndexesMap_.at(ind));
}

TSkiffDataConstPtr TSkiffStruct::GetSkiffDataPtr(std::string_view ind) const {
    return GetSkiffDataPtr(IndexesMap_.at(ind));
}

TSkiffDataPtr& TSkiffStruct::GetSkiffDataPtr(std::string_view ind) {
    return GetSkiffDataPtr(IndexesMap_.at(ind));
}

NTi::TTypePtr TSkiffStruct::GetChildType(std::string_view ind) const {
    return GetSchema()->StripTags()->AsStruct()->GetMember(ind).GetType();
}

NTi::TTypePtr TSkiffStruct::GetChildType(size_t ind) const {
    return GetSchema()->StripTags()->AsStruct()->GetMembers()[ind].GetType();
}

// TSkiffDict

IDictPtr TSkiffDict::CopyDict() const {
    return std::make_shared<TSkiffDict>(*this);
}

NTi::TTypePtr TSkiffDict::GetChildType(size_t) const {
    auto dictSchema = GetSchema()->StripTags()->AsDict();
    return NTi::Struct({NTi::TStructType::TOwnedMember("key", dictSchema->GetKeyType()),
                       NTi::TStructType::TOwnedMember("value", dictSchema->GetValueType())});
}

// TSkiffRow

TSkiffRow::TSkiffRow(NTi::TTypePtr schema) : TSkiffStruct(std::move(schema)) { }

TSkiffRow::TSkiffRow(NTi::TTypePtr schema, TBuffer&& buf)
  : TSkiffStruct(std::move(schema), std::move(buf)) { }

TSkiffRow::TSkiffRow(NTi::TTypePtr schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets)
  : TSkiffStruct(std::move(schema), std::move(buf), std::move(fieldsOffsets)) { }

TSkiffRow::TSkiffRow(const NYT::TTableSchema& schema)
  : TSkiffRow(TableSchemaToStructType(schema)) { }

TSkiffRow::TSkiffRow(const NYT::TTableSchema& schema, TBuffer&& buf)
  : TSkiffRow(TableSchemaToStructType(schema), std::move(buf)) { }

TSkiffRow::TSkiffRow(const NYT::TTableSchema& schema, TBuffer&& buf, std::vector<ptrdiff_t> fieldsOffsets)
  : TSkiffRow(TableSchemaToStructType(schema), std::move(buf), std::move(fieldsOffsets)) { }

TSkiffRow::TSkiffRow(const TSkiffRow& rhs) : TSkiffStruct(rhs) { }

TSkiffRow::TSkiffRow(TSkiffRow&& rhs) : TSkiffStruct(std::move(rhs)) { }

TSkiffRow& TSkiffRow::operator=(const TSkiffRow& rhs) {
    TSkiffStruct::operator=(rhs);
    return *this;
}

TSkiffRow& TSkiffRow::operator=(TSkiffRow&& rhs) {
    TSkiffStruct::operator=(std::move(rhs));
    return *this;
}

IRowPtr TSkiffRow::CopyRow() const {
    return std::make_shared<TSkiffRow>(*this); 
}

NTi::TTypePtr TSkiffRow::GetChildType(size_t ind) const {
    return GetSchema()->StripTags()->AsStruct()->GetMembers()[ind].GetType();
}

}
