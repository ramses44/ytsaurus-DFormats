#include "protobuf_types.h"

namespace DFormats {

Message* CopyMessage(const Message& src) {
    Message* dst = src.New();
    dst->CopyFrom(src);
    return dst;
}

template <typename IndexType>
void SetDefaultValue(IBaseIndexed<IndexType>* dst, IndexType ind, const FieldDescriptor* fieldDesc, std::shared_ptr<MessageFactory> messageFactory) {
    switch (fieldDesc->cpp_type()) {
    case FieldDescriptor::CppType::CPPTYPE_INT32:
        dst->template SetValue<int32_t>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_INT64:
        dst->template SetValue<int64_t>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_UINT32:
        dst->template SetValue<uint32_t>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_UINT64:
        dst->template SetValue<uint64_t>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
        dst->template SetValue<double>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_FLOAT:
        dst->template SetValue<float>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_BOOL:
        dst->template SetValue<bool>(ind, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_STRING:
        dst->template SetValue<std::string>(ind, "");
        break;
    case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
    {
        std::shared_ptr<Message> msg(messageFactory->GetPrototype(fieldDesc->message_type())->New());
        dst->template SetValue<IStructPtr>(ind, std::make_shared<TProtobufStruct>(msg, messageFactory));
        break;
    }
    default:
        break;
    }
}

// TProtobufObject

TProtobufObject::TProtobufObject(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory)
  : Underlying_(underlying)
  , Factory_(factory)
  , ScratchStrings_(Underlying_->GetDescriptor()->field_count()) { }

TProtobufObject::TProtobufObject(const TProtobufObject& rhs)
  : Underlying_(CopyMessage(*rhs.Underlying_))  // Скопировать в новое сообщение
  , Factory_(rhs.Factory_)
  , ScratchStrings_(Underlying_->GetDescriptor()->field_count()) { }

TProtobufObject::TProtobufObject(TProtobufObject&& rhs)
  : Underlying_(std::move(rhs.Underlying_))
  , Factory_(std::move(rhs.Factory_))
  , ScratchStrings_(std::move(rhs.ScratchStrings_)) { }
    
TProtobufObject& TProtobufObject::operator=(const TProtobufObject& rhs) {
    Underlying_.reset(CopyMessage(*rhs.Underlying_));
    Factory_ = rhs.Factory_;
    ScratchStrings_ = rhs.ScratchStrings_;

    return *this;
}

TProtobufObject& TProtobufObject::operator=(TProtobufObject&& rhs){
    Underlying_ = std::move(rhs.Underlying_);
    Factory_ = std::move(rhs.Factory_);
    ScratchStrings_ = std::move(rhs.ScratchStrings_);

    return *this;
}

std::shared_ptr<Message> TProtobufObject::Release() {
    return std::move(Underlying_);
}

const Message* TProtobufObject::RawMessage() const {
    return Underlying_.get();
}

Message* TProtobufObject::RawMessage() {
    return Underlying_.get();
}

bool TProtobufObject::GetBool(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetBool(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedBool(*RawMessage(), GetFieldDescriptor(0), ind);
}

int8_t TProtobufObject::GetInt8(size_t ind) const {
    return GetInt32(ind);
}

int16_t TProtobufObject::GetInt16(size_t ind) const {
    return GetInt32(ind);
}

int32_t TProtobufObject::GetInt32(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetInt32(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedInt32(*RawMessage(), GetFieldDescriptor(0), ind);
}

int64_t TProtobufObject::GetInt64(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetInt64(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedInt64(*RawMessage(), GetFieldDescriptor(0), ind);
}

uint8_t TProtobufObject::GetUInt8(size_t ind) const {
    return GetUInt32(ind);
}

uint16_t TProtobufObject::GetUInt16(size_t ind) const {
    return GetUInt32(ind);
}

uint32_t TProtobufObject::GetUInt32(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetUInt32(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedUInt32(*RawMessage(), GetFieldDescriptor(0), ind);
}

uint64_t TProtobufObject::GetUInt64(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetUInt64(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedUInt64(*RawMessage(), GetFieldDescriptor(0), ind);
}

float TProtobufObject::GetFloat(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetFloat(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedFloat(*RawMessage(), GetFieldDescriptor(0), ind);
}

double TProtobufObject::GetDouble(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetDouble(*RawMessage(), GetFieldDescriptor(ind))
           : GetReflection()->GetRepeatedDouble(*RawMessage(), GetFieldDescriptor(0), ind);
}

std::string_view TProtobufObject::GetString(size_t ind) const {
    return !IsRepeated()
           ? GetReflection()->GetStringReference(*RawMessage(), GetFieldDescriptor(ind), GetScratchString(ind))
           : GetReflection()->GetRepeatedStringReference(*RawMessage(), GetFieldDescriptor(0), ind, GetScratchString(ind));
}

IStructConstPtr TProtobufObject::GetStruct(size_t ind) const {
    const auto& data = !IsRepeated()
                       ? GetReflection()->GetMessage(*RawMessage(), GetFieldDescriptor(ind), Factory().get())
                       : GetReflection()->GetRepeatedMessage(*RawMessage(), GetFieldDescriptor(0), ind);

    return std::make_shared<TProtobufStruct>(std::shared_ptr<Message>(CopyMessage(data)), Factory());
}

ITupleConstPtr TProtobufObject::GetTuple(size_t) const {
    ythrow yexception() << "Tuples are not supported in Protobuf";
}

IListConstPtr TProtobufObject::GetList(size_t ind) const {
    return std::make_shared<TProtobufList>(GetMessage(), Factory(), ind);
}

IDictConstPtr TProtobufObject::GetDict(size_t ind) const {
    return std::make_shared<TProtobufDict>(GetMessage(), Factory(), ind);
}

IVariantConstPtr TProtobufObject::GetVariant(size_t ind) const {
    const auto& data = !IsRepeated()
                       ? GetReflection()->GetMessage(*RawMessage(), GetFieldDescriptor(ind), Factory().get())
                       : GetReflection()->GetRepeatedMessage(*RawMessage(), GetFieldDescriptor(0), ind);

    return std::make_shared<TProtobufVariant>(std::shared_ptr<Message>(CopyMessage(data)), Factory());
}

IOptionalConstPtr TProtobufObject::GetOptional(size_t ind) const {
    return std::make_shared<TProtobufOptional>(GetMessage(), Factory(), ind);
}

void TProtobufObject::SetBool(size_t ind, bool value) {
    if (!IsRepeated())
        GetReflection()->SetBool(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedBool(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetInt8(size_t ind, int8_t value) {
    SetInt32(ind, value);
}

void TProtobufObject::SetInt16(size_t ind, int16_t value) {
    SetInt32(ind, value);
}

void TProtobufObject::SetInt32(size_t ind, int32_t value) {
    if (!IsRepeated())
        GetReflection()->SetInt32(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedInt32(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetInt64(size_t ind, int64_t value) {
    if (!IsRepeated())
        GetReflection()->SetInt64(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedInt64(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetUInt8(size_t ind, uint8_t value) {
    SetUInt32(ind, value);
}

void TProtobufObject::SetUInt16(size_t ind, uint16_t value) {
    SetUInt32(ind, value);
}

void TProtobufObject::SetUInt32(size_t ind, uint32_t value) {
    if (!IsRepeated())
        GetReflection()->SetUInt32(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedUInt32(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetUInt64(size_t ind, uint64_t value) {
    if (!IsRepeated())
        GetReflection()->SetUInt64(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedUInt64(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetFloat(size_t ind, float value) {
    if (!IsRepeated())
        GetReflection()->SetFloat(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedFloat(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetDouble(size_t ind, double value) {
    if (!IsRepeated())
        GetReflection()->SetDouble(RawMessage(), GetFieldDescriptor(ind), value);
    else
        GetReflection()->SetRepeatedDouble(RawMessage(), GetFieldDescriptor(0), ind, value);
}

void TProtobufObject::SetString(size_t ind, std::string_view value) {
    if (!IsRepeated())
        GetReflection()->SetString(RawMessage(), GetFieldDescriptor(ind), TString(value));
    else
        GetReflection()->SetRepeatedString(RawMessage(), GetFieldDescriptor(0), ind, TString(value));
}

void TProtobufObject::SetStruct(size_t ind, IStructConstPtr value) {
    auto data = std::dynamic_pointer_cast<const TProtobufStruct>(value)->RawMessage();
    if (!IsRepeated())
        GetReflection()->MutableMessage(RawMessage(), GetFieldDescriptor(ind), Factory().get())->CopyFrom(*data);
    else
        GetReflection()->MutableRepeatedMessage(RawMessage(), GetFieldDescriptor(0), ind)->CopyFrom(*data);
}

void TProtobufObject::SetTuple(size_t, ITupleConstPtr) {
    ythrow yexception() << "Tuples are not supported in Protobuf";
}

void TProtobufObject::SetList(size_t ind, IListConstPtr value) {
    auto data = std::dynamic_pointer_cast<TProtobufList>(value->CopyList())->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

void TProtobufObject::SetDict(size_t ind, IDictConstPtr value) {
    auto data = std::dynamic_pointer_cast<TProtobufDict>(value->CopyDict())->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

void TProtobufObject::SetVariant(size_t ind, IVariantConstPtr value) {
    auto data = std::dynamic_pointer_cast<const TProtobufVariant>(value)->RawMessage();
    if (!IsRepeated())
        GetReflection()->MutableMessage(RawMessage(), GetFieldDescriptor(ind), Factory().get())->CopyFrom(*data);
    else
        GetReflection()->MutableRepeatedMessage(RawMessage(), GetFieldDescriptor(0), ind)->CopyFrom(*data);
}

void TProtobufObject::SetOptional(size_t ind, IOptionalConstPtr value) {
    auto data = std::dynamic_pointer_cast<TProtobufOptional>(value->CopyOptional())->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

void TProtobufObject::SetString(size_t ind, std::string&& value) {
    if (!IsRepeated())
        GetReflection()->SetString(RawMessage(), GetFieldDescriptor(ind), TString(std::move(value)));
    else
        GetReflection()->SetRepeatedString(RawMessage(), GetFieldDescriptor(0), ind, TString(std::move(value)));
}

void TProtobufObject::SetStruct(size_t ind, IStructPtr&& value) {
    auto data = std::dynamic_pointer_cast<TProtobufStruct>(value)->RawMessage();
    if (!IsRepeated())
        GetReflection()->MutableMessage(RawMessage(), GetFieldDescriptor(ind), Factory().get())->CopyFrom(*data);
    else
        GetReflection()->MutableRepeatedMessage(RawMessage(), GetFieldDescriptor(0), ind)->CopyFrom(*data);
}

void TProtobufObject::SetTuple(size_t, ITuplePtr&&) {
    ythrow yexception() << "Tuples are not supported in Protobuf";
}

void TProtobufObject::SetList(size_t ind, IListPtr&& value) {
    auto data = std::dynamic_pointer_cast<TProtobufList>(value)->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

void TProtobufObject::SetDict(size_t ind, IDictPtr&& value) {
    auto data = std::dynamic_pointer_cast<TProtobufDict>(value)->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

void TProtobufObject::SetVariant(size_t ind, IVariantPtr&& value) {
    auto data = std::dynamic_pointer_cast<TProtobufVariant>(value)->RawMessage();
    if (!IsRepeated())
        GetReflection()->MutableMessage(RawMessage(), GetFieldDescriptor(ind), Factory().get())->CopyFrom(*data);
    else
        GetReflection()->MutableRepeatedMessage(RawMessage(), GetFieldDescriptor(0), ind)->CopyFrom(*data);
}

void TProtobufObject::SetOptional(size_t ind, IOptionalPtr&& value) {
    auto data = std::dynamic_pointer_cast<TProtobufOptional>(value)->Release();
    GetReflection()->SwapFields(RawMessage(), data.get(), {GetFieldDescriptor(ind)});
}

bool TProtobufObject::IsRepeated() const {
    return false;
}

const FieldDescriptor* TProtobufObject::GetFieldDescriptor(size_t ind) const {
    return RawMessage()->GetDescriptor()->field(ind);
}

const Reflection* TProtobufObject::GetReflection() const {
    return RawMessage()->GetReflection();
}

std::shared_ptr<Message> TProtobufObject::GetMessage() const {
    return Underlying_;
}

TString* TProtobufObject::GetScratchString(size_t ind) const {
    if (!ScratchStrings_[ind].has_value()) {
        ScratchStrings_[ind].emplace();
    }

    return &ScratchStrings_[ind].value(); 
}

std::shared_ptr<MessageFactory> TProtobufObject::Factory() const {
    return Factory_;
}

// TProtobufStruct

TProtobufStruct::TProtobufStruct(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory)
  : TProtobufObject(underlying, factory) { }

TProtobufStruct::TProtobufStruct(const TProtobufStruct& rhs)
  : TProtobufObject(rhs), IndexesMap_(rhs.IndexesMap_) { }

TProtobufStruct::TProtobufStruct(TProtobufStruct&& rhs)
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs)), IndexesMap_(std::move(rhs.IndexesMap_)) { }

TProtobufStruct& TProtobufStruct::operator=(const TProtobufStruct& rhs) {
    TProtobufObject::operator=(rhs);
    IndexesMap_ = rhs.IndexesMap_;

    return *this;
}

TProtobufStruct& TProtobufStruct::operator=(TProtobufStruct&& rhs) {
    TProtobufObject::operator=(static_cast<TProtobufObject&&>(rhs));
    IndexesMap_ = std::move(rhs.IndexesMap_);
    
    return *this;
}

size_t TProtobufStruct::GetIndex(std::string_view ind) const {
    std::string name(ind);

    if (!IndexesMap_.contains(name)) {
        IndexesMap_[name] = RawMessage()->GetDescriptor()->FindFieldByName(name)->index();
    }
    return IndexesMap_[name];
}

IStructPtr TProtobufStruct::CopyStruct() const {
    return std::make_shared<TProtobufStruct>(*this);
}

std::vector<std::string> TProtobufStruct::FieldsNames() const {
    auto descriptor = RawMessage()->GetDescriptor();

    std::vector<std::string> res;
    res.reserve(descriptor->field_count());

    for (int i = 0; i < descriptor->field_count(); ++i) {
        res.emplace_back(descriptor->field(i)->name());
    }

    return res;
}

// TProtobufVariant

TProtobufVariant::TProtobufVariant(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory)
  : TProtobufObject(underlying, factory), TProtobufStruct(underlying, factory) { }

TProtobufVariant::TProtobufVariant(const TProtobufVariant& rhs)
  : TProtobufObject(rhs), TProtobufStruct(rhs) { }

TProtobufVariant::TProtobufVariant(TProtobufVariant&& rhs)
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs))
  , TProtobufStruct(static_cast<TProtobufStruct&&>(rhs)) { }

TProtobufVariant& TProtobufVariant::operator=(const TProtobufVariant& rhs) {
    TProtobufStruct::operator=(rhs);
    return *this;
}

TProtobufVariant& TProtobufVariant::operator=(TProtobufVariant&& rhs) {
    TProtobufStruct::operator=(static_cast<TProtobufStruct&&>(rhs));
    return *this;
}

IVariantPtr TProtobufVariant::CopyVariant() const {
    return std::make_shared<TProtobufVariant>(*this);
}

size_t TProtobufVariant::VariantsCount() const {
    return RawMessage()->GetDescriptor()->field_count();
}

size_t TProtobufVariant::VariantNumber() const {
    auto oneofDesc = RawMessage()->GetDescriptor()->oneof_decl(0);
    auto currentField = GetReflection()->GetOneofFieldDescriptor(*RawMessage(), oneofDesc);
    return currentField->index();
}

void TProtobufVariant::EmplaceVariant(size_t number) {
    SetDefaultValue(dynamic_cast<IBaseIndexed<size_t>*>(this), number, GetFieldDescriptor(number), Factory());
}

// TProtobufOptional

TProtobufOptional::TProtobufOptional(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex)
  : TProtobufObject(underlying, factory), FieldIndex_(fieldIndex) { }

TProtobufOptional::TProtobufOptional(const TProtobufOptional& rhs)
  : TProtobufObject(rhs), FieldIndex_(rhs.FieldIndex_) { }

TProtobufOptional::TProtobufOptional(TProtobufOptional&& rhs)
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs)), FieldIndex_(rhs.FieldIndex_) { }

TProtobufOptional& TProtobufOptional::operator=(const TProtobufOptional& rhs) {
    TProtobufObject::operator=(rhs);
    FieldIndex_ = rhs.FieldIndex_;
    
    return *this;
}

TProtobufOptional& TProtobufOptional::operator=(TProtobufOptional&& rhs) {
    TProtobufObject::operator=(static_cast<TProtobufObject&&>(rhs));
    FieldIndex_ = rhs.FieldIndex_;

    return *this;
}

IOptionalPtr TProtobufOptional::CopyOptional() const {
    return std::make_shared<TProtobufOptional>(*this);
}

size_t TProtobufOptional::GetIndex(bool) const {
    return FieldIndex_;
}

bool TProtobufOptional::HasValue() const {
    return GetReflection()->HasField(*RawMessage(), GetFieldDescriptor(GetIndex(0)));
}

void TProtobufOptional::ClearValue() {
    return GetReflection()->ClearField(RawMessage(), GetFieldDescriptor(GetIndex(0)));
}

void TProtobufOptional::EmplaceValue() {
    SetDefaultValue(dynamic_cast<IBaseIndexed<size_t>*>(this), GetIndex(0), GetFieldDescriptor(GetIndex(0)), Factory());
}

// TProtobufList

TProtobufList::TProtobufList(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex)
  : TProtobufObject(underlying, factory), FieldIndex_(fieldIndex) { }

TProtobufList::TProtobufList(const TProtobufList& rhs)
  : TProtobufObject(rhs), FieldIndex_(rhs.FieldIndex_) { }

TProtobufList::TProtobufList(TProtobufList&& rhs)
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs)), FieldIndex_(rhs.FieldIndex_) { }

TProtobufList& TProtobufList::operator=(const TProtobufList& rhs) {
    TProtobufObject::operator=(rhs);
    FieldIndex_ = rhs.FieldIndex_;
    
    return *this;
}

TProtobufList& TProtobufList::operator=(TProtobufList&& rhs) {
    TProtobufObject::operator=(std::move(rhs));
    FieldIndex_ = rhs.FieldIndex_;

    return *this;
}

IListPtr TProtobufList::CopyList() const {
    return std::make_shared<TProtobufList>(*this);
}

size_t TProtobufList::Size() const {
    return GetReflection()->FieldSize(*RawMessage(), GetFieldDescriptor(FieldIndex()));
}

void TProtobufList::Clear() {
    GetReflection()->ClearField(RawMessage(), GetFieldDescriptor(FieldIndex()));
}

void TProtobufList::PopBack() {
    GetReflection()->RemoveLast(RawMessage(), GetFieldDescriptor(FieldIndex()));
}

void TProtobufList::Extend() {
    auto fieldDesc = GetFieldDescriptor(FieldIndex());

    switch (fieldDesc->cpp_type()) {
    case FieldDescriptor::CppType::CPPTYPE_INT32:
        GetReflection()->AddInt32(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_INT64:
        GetReflection()->AddInt64(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_UINT32:
        GetReflection()->AddUInt32(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_UINT64:
        GetReflection()->AddUInt64(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
        GetReflection()->AddDouble(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_FLOAT:
        GetReflection()->AddFloat(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_BOOL:
        GetReflection()->AddBool(RawMessage(), fieldDesc, 0);
        break;
    case FieldDescriptor::CppType::CPPTYPE_STRING:
        GetReflection()->AddString(RawMessage(), fieldDesc, "");
        break;
    case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
        GetReflection()->AddMessage(RawMessage(), fieldDesc, Factory().get());
        break;
    default:
        break;
    }
}

const FieldDescriptor* TProtobufList::GetFieldDescriptor(size_t) const {
    return TProtobufObject::GetFieldDescriptor(FieldIndex());
}

bool TProtobufList::IsRepeated() const {
    return true;
}

// TProtobufDict

TProtobufDict::TProtobufDict(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory, size_t fieldIndex)
  : TProtobufObject(underlying, factory), TProtobufList(underlying, factory, fieldIndex) { }

TProtobufDict::TProtobufDict(const TProtobufDict& rhs) : TProtobufObject(rhs), TProtobufList(rhs) { }

TProtobufDict::TProtobufDict(TProtobufDict&& rhs) 
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs))
  , TProtobufList(static_cast<TProtobufList&&>(rhs)) { }

TProtobufDict& TProtobufDict::operator=(const TProtobufDict& rhs) {
    TProtobufList::operator=(rhs);
    return *this;
}

TProtobufDict& TProtobufDict::operator=(TProtobufDict&& rhs) {
    TProtobufList::operator=(std::move(rhs));
    return *this;
}

// TProtobufRow

TProtobufRow::TProtobufRow(std::shared_ptr<Message> underlying, std::shared_ptr<MessageFactory> factory)
  : TProtobufObject(underlying, factory), TProtobufStruct(underlying, factory) { }

TProtobufRow::TProtobufRow(const TProtobufRow& rhs) : TProtobufObject(rhs), TProtobufStruct(rhs) { }

TProtobufRow::TProtobufRow(TProtobufRow&& rhs)
  : TProtobufObject(static_cast<TProtobufObject&&>(rhs))
  , TProtobufStruct(static_cast<TProtobufStruct&&>(rhs)) { }

TProtobufRow& TProtobufRow::operator=(const TProtobufRow& rhs) {
    TProtobufStruct::operator=(rhs);
    return *this;
}

TProtobufRow& TProtobufRow::operator=(TProtobufRow&& rhs) {
    TProtobufStruct::operator=(std::move(rhs));
    return *this;
}

}
