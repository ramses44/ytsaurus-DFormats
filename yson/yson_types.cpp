#include "yson_types.h"

namespace DFormats {

TNode ConstructNode(NTi::TTypePtr schema) {
    switch (schema->GetTypeName()) {
    case NTi::ETypeName::Bool:
        return TNode(false);
    case NTi::ETypeName::Int8:
    case NTi::ETypeName::Int16:
    case NTi::ETypeName::Int32:
    case NTi::ETypeName::Int64:
    case NTi::ETypeName::Interval:
    case NTi::ETypeName::Interval64:
        return TNode(static_cast<int64_t>(0));
    case NTi::ETypeName::Uint8:
    case NTi::ETypeName::Uint16:
    case NTi::ETypeName::Uint32:
    case NTi::ETypeName::Uint64:
    case NTi::ETypeName::Date:
    case NTi::ETypeName::Date32:
    case NTi::ETypeName::TzDate:
    case NTi::ETypeName::Datetime:
    case NTi::ETypeName::Datetime64:
    case NTi::ETypeName::TzDatetime:
    case NTi::ETypeName::Timestamp:
    case NTi::ETypeName::Timestamp64:
    case NTi::ETypeName::TzTimestamp:
        return TNode(static_cast<uint64_t>(0));
    case NTi::ETypeName::Float:
    case NTi::ETypeName::Double:
        return TNode(static_cast<double>(0));
    case NTi::ETypeName::String:
    case NTi::ETypeName::Utf8:
        return TNode("");
    case NTi::ETypeName::Decimal:
    case NTi::ETypeName::Json:
    case NTi::ETypeName::Uuid:
        return TNode("");  // Generates invalid data
    case NTi::ETypeName::List:
    case NTi::ETypeName::Dict:
        return TNode::CreateList();
    case NTi::ETypeName::Tuple:
    {
        auto res = TNode::CreateList();
        for (const auto& field : schema->AsTuple()->GetElements()) {
            res.Add(ConstructNode(field.GetType()));
        }
        return res;
    }
    case NTi::ETypeName::Struct:
    {
        auto res = TNode::CreateMap();
        for (const auto& field : schema->AsStruct()->GetMembers()) {
            res[field.GetName()] = ConstructNode(field.GetType());
        }
        return res;
    }
    case NTi::ETypeName::Variant:
        if (schema->AsVariant()->IsVariantOverTuple()) 
            return TNode()
                .Add(static_cast<uint64_t>(0))
                .Add(ConstructNode(schema->AsVariant()->GetUnderlyingType()->AsTuple()->GetElements()[0].GetType()));
        else
            return TNode()
                .Add(schema->AsVariant()->GetUnderlyingType()->AsStruct()->GetMembers()[0].GetName())
                .Add(ConstructNode(schema->AsVariant()->GetUnderlyingType()->AsStruct()->GetMembers()[0].GetType()));
    case NTi::ETypeName::Optional:
        return TNode::CreateEntity();
    case NTi::ETypeName::Tagged:
        return ConstructNode(schema->AsTagged()->GetItemType());
    default:
        return TNode();
    }
}

// TYsonData

TYsonData::TYsonData() : Schema_(NTi::Void()) { }

TYsonData::TYsonData(NTi::TTypePtr schema)
  : Schema_(schema), Underlying_(ConstructNode(Schema_)) { }

TYsonData::TYsonData(NTi::TTypePtr schema, TNode underlying)
  : Schema_(schema), Underlying_(std::move(underlying)) { }

TYsonData::TYsonData(const TYsonData& rhs) : Schema_(rhs.Schema_), Underlying_(rhs.Underlying_) { }

TYsonData::TYsonData(TYsonData&& rhs)
  : Schema_(std::move(rhs.Schema_)), Underlying_(std::move(rhs.Underlying_)) { }
    
TYsonData& TYsonData::operator=(const TYsonData& rhs) {
    Underlying_ = rhs.Underlying_;
    Schema_ = rhs.Schema_;
    return *this;
}

TYsonData& TYsonData::operator=(TYsonData&& rhs) {
    Underlying_ = std::move(rhs.Underlying_);
    Schema_ = std::move(rhs.Schema_);
    return *this;
}

const TNode& TYsonData::Underlying() const {
    return Underlying_;
}

TNode& TYsonData::Underlying() {
    return Underlying_;
}

TNode&& TYsonData::Release() {
    return std::move(Underlying_);
}

NTi::TTypePtr TYsonData::GetSchema() const {
    return Schema_;
}

// TYsonStruct

TYsonStruct::TYsonStruct() : TYsonData(NTi::Struct({})) { }

TYsonStruct::TYsonStruct(NTi::TTypePtr schema) : TYsonData(schema) { }

TYsonStruct::TYsonStruct(NTi::TTypePtr schema, TNode underlying)
  : TYsonData(schema, std::move(underlying)) { }

TYsonStruct::TYsonStruct(const TYsonStruct& rhs) : TYsonData(rhs) { }

TYsonStruct::TYsonStruct(TYsonStruct&& rhs) : TYsonData(std::move(rhs)) { }

TYsonStruct& TYsonStruct::operator=(const TYsonStruct& rhs) {
    TYsonData::operator=(rhs);
    return *this;
}

TYsonStruct& TYsonStruct::operator=(TYsonStruct&& rhs) {
    TYsonData::operator=(std::move(rhs));
    return *this;
}

IStructPtr TYsonStruct::CopyStruct() const {
    return std::make_shared<TYsonStruct>(*this);
}

std::vector<std::string> TYsonStruct::FieldsNames() const {
    std::vector<std::string> res;
    res.reserve(Underlying().AsMap().size());
    
    for (const auto& field : GetSchema()->AsStruct()->GetMembers()) {
        res.emplace_back(field.GetName());
    }

    return std::move(res);
}

NTi::TTypePtr TYsonStruct::GetSchema(std::string_view ind) const {
    return GetSchema()->AsStruct()->GetMember(ind).GetType();
}

const TNode& TYsonStruct::GetData(std::string_view ind) const {
    return Underlying()[ind];
}

TNode& TYsonStruct::GetData(std::string_view ind) {
    return Underlying()[ind];
}

// TYsonTuple

TYsonTuple::TYsonTuple() : TYsonData(NTi::Tuple({})) { }

TYsonTuple::TYsonTuple(NTi::TTypePtr schema) : TYsonData(schema) { }

TYsonTuple::TYsonTuple(NTi::TTypePtr schema, TNode underlying)
  : TYsonData(schema, std::move(underlying)) { }

TYsonTuple::TYsonTuple(const TYsonTuple& rhs) : TYsonData(rhs) { }

TYsonTuple::TYsonTuple(TYsonTuple&& rhs) : TYsonData(std::move(rhs)) { }

TYsonTuple& TYsonTuple::operator=(const TYsonTuple& rhs) {
    TYsonData::operator=(rhs);
    return *this;
}

TYsonTuple& TYsonTuple::operator=(TYsonTuple&& rhs) {
    TYsonData::operator=(std::move(rhs));
    return *this;
}

ITuplePtr TYsonTuple::CopyTuple() const {
    return std::make_shared<TYsonTuple>(*this);
}

size_t TYsonTuple::FieldsCount() const {
    return Underlying().AsList().size();
}

NTi::TTypePtr TYsonTuple::GetSchema(size_t ind) const {
    return GetSchema()->AsTuple()->GetElements()[ind].GetType();
}

const TNode& TYsonTuple::GetData(size_t ind) const {
    Y_ENSURE(ind < Underlying().Size(), "Tuple field with that index does not exist");
    return Underlying()[ind];
}

TNode& TYsonTuple::GetData(size_t ind) {
    while (Underlying().Size() <= ind) {
        Underlying().Add();
    }
    return Underlying()[ind];
}

// TYsonList

TYsonList::TYsonList() : TYsonData(NTi::List(NTi::Void())) { }

TYsonList::TYsonList(NTi::TTypePtr schema) : TYsonData(schema) { }

TYsonList::TYsonList(NTi::TTypePtr schema, TNode underlying)
  : TYsonData(schema, std::move(underlying)) { }

TYsonList::TYsonList(const TYsonList& rhs) : TYsonData(rhs) { }

TYsonList::TYsonList(TYsonList&& rhs) : TYsonData(std::move(rhs)) { }

TYsonList& TYsonList::operator=(const TYsonList& rhs) {
    TYsonData::operator=(rhs);
    return *this;
}

TYsonList& TYsonList::operator=(TYsonList&& rhs) {
    TYsonData::operator=(std::move(rhs));
    return *this;
}

IListPtr TYsonList::CopyList() const {
    return std::make_shared<TYsonList>(*this);
}

size_t TYsonList::Size() const {
    return Underlying().AsList().size();
}

void TYsonList::Clear() {
    Underlying() = TNode::CreateList();
}

void TYsonList::PopBack() {
    Underlying().AsList().pop_back();
}

void TYsonList::Extend() {
    Underlying().Add(ConstructNode(GetSchema(0)));
}

NTi::TTypePtr TYsonList::GetSchema(size_t) const {
    return GetSchema()->AsList()->GetItemType();
}

const TNode& TYsonList::GetData(size_t ind) const {
    return Underlying()[ind];
}

TNode& TYsonList::GetData(size_t ind) {
    return Underlying()[ind];
}

// TYsonVariant

TYsonVariant::TYsonVariant() : TYsonData(NTi::Variant(NTi::Tuple({{NTi::Void()}}))) { }

TYsonVariant::TYsonVariant(NTi::TTypePtr schema) : TYsonData(schema) { }

TYsonVariant::TYsonVariant(NTi::TTypePtr schema, TNode underlying)
  : TYsonData(schema, std::move(underlying)) { }

TYsonVariant::TYsonVariant(const TYsonVariant& rhs) : TYsonData(rhs) { }

TYsonVariant::TYsonVariant(TYsonVariant&& rhs) : TYsonData(std::move(rhs)) { }

TYsonVariant& TYsonVariant::operator=(const TYsonVariant& rhs) {
    TYsonData::operator=(rhs);
    return *this;
}

TYsonVariant& TYsonVariant::operator=(TYsonVariant&& rhs) {
    TYsonData::operator=(std::move(rhs));
    return *this;
}

IVariantPtr TYsonVariant::CopyVariant() const {
    return std::make_shared<TYsonVariant>(*this);
}

size_t TYsonVariant::VariantsCount() const  {
    ythrow yexception() << "Not implemented";
}

size_t TYsonVariant::VariantNumber() const {
    if (GetSchema()->AsVariant()->IsVariantOverTuple()) {
        return Underlying()[0].AsUint64();
    }
    return GetSchema()->AsVariant()->GetUnderlyingType()->AsStruct()->GetMemberIndex(Underlying()[0].AsString());
}

void TYsonVariant::EmplaceVariant(size_t number) {
    Underlying()[0] = GetSchema()->AsVariant()->IsVariantOverTuple() ? TNode(number) : TNode(
        GetSchema()->AsVariant()->GetUnderlyingType()->AsStruct()->GetMembers()[number].GetName());
    Underlying()[1] = ConstructNode(GetSchema(number));
}

NTi::TTypePtr TYsonVariant::GetSchema(size_t ind) const {
    return GetSchema()->AsVariant()->IsVariantOverTuple() ?
        GetSchema()->AsVariant()->GetUnderlyingType()->AsTuple()->GetElements()[ind].GetType() :
        GetSchema()->AsVariant()->GetUnderlyingType()->AsStruct()->GetMembers()[ind].GetType();
}

const TNode& TYsonVariant::GetData(size_t) const {
    return Underlying()[1];
}

TNode& TYsonVariant::GetData(size_t) {
    return Underlying()[1];
}

NTi::TTypePtr TYsonVariant::GetSchema(std::string_view ind) const {
    return GetSchema()->AsVariant()->GetUnderlyingType()->AsStruct()->GetMember(ind).GetType();
}

const TNode& TYsonVariant::GetData(std::string_view) const {
    return Underlying()[1];
}

TNode& TYsonVariant::GetData(std::string_view) {
    return Underlying()[1];
}

// TYsonOptional

TYsonOptional::TYsonOptional() : TYsonData(NTi::Optional(NTi::Void())) { }

TYsonOptional::TYsonOptional(NTi::TTypePtr schema) : TYsonData(schema) { }

TYsonOptional::TYsonOptional(NTi::TTypePtr schema, TNode underlying)
  : TYsonData(schema, std::move(underlying)) { }

TYsonOptional::TYsonOptional(const TYsonOptional& rhs) : TYsonData(rhs) { }

TYsonOptional::TYsonOptional(TYsonOptional&& rhs) : TYsonData(std::move(rhs)) { }

TYsonOptional& TYsonOptional::operator=(const TYsonOptional& rhs) {
    TYsonData::operator=(rhs);
    return *this;
}

TYsonOptional& TYsonOptional::operator=(TYsonOptional&& rhs) {
    TYsonData::operator=(std::move(rhs));
    return *this;
}

IOptionalPtr TYsonOptional::CopyOptional() const {
    return std::make_shared<TYsonOptional>(*this);
}

bool TYsonOptional::HasValue() const {
    return Underlying().HasValue();
}

void TYsonOptional::ClearValue() {
    Underlying() = TNode::CreateEntity();
}

void TYsonOptional::EmplaceValue() {
    Underlying() = ConstructNode(GetSchema(true));
}

NTi::TTypePtr TYsonOptional::GetSchema(bool) const {
    return GetSchema()->AsOptional()->GetItemType();
}

const TNode& TYsonOptional::GetData(bool) const {
    return Underlying();
}

TNode& TYsonOptional::GetData(bool) {
    return Underlying();
}

// TYsonDict

TYsonDict::TYsonDict() : TYsonList(NTi::Dict(NTi::Void(), NTi::Void())) { }

TYsonDict::TYsonDict(NTi::TTypePtr schema) : TYsonList(schema) { }

TYsonDict::TYsonDict(NTi::TTypePtr schema, TNode underlying)
  : TYsonList(schema, std::move(underlying)) { }

TYsonDict::TYsonDict(const TYsonDict& rhs) : TYsonList(rhs) { }

TYsonDict::TYsonDict(TYsonDict&& rhs) : TYsonList(std::move(rhs)) { }

TYsonDict& TYsonDict::operator=(const TYsonDict& rhs) {
    TYsonList::operator=(rhs);
    return *this;
}

TYsonDict& TYsonDict::operator=(TYsonDict&& rhs) {
    TYsonList::operator=(std::move(rhs));
    return *this;
}

IDictPtr TYsonDict::CopyDict() const {
    return std::make_shared<TYsonDict>(*this);
}

void TYsonDict::Extend() {
    Underlying().Add(ConstructNode(NTi::Tuple(
        {{GetSchema()->AsDict()->GetKeyType()}, {GetSchema()->AsDict()->GetValueType()}})));
}

IStructConstPtr TYsonDict::GetStruct(size_t ind) const {
    const auto& kv = Underlying()[ind];

    Y_ENSURE(kv.IsList() && kv.Size() == 2, "Invalid TNode as Key-Value dict entry");

    return std::make_shared<TYsonStruct>(
        NTi::Struct({{"key", GetSchema()->AsDict()->GetKeyType()},
                     {"value", GetSchema()->AsDict()->GetValueType()}}),
         TNode()("key", kv[0])("value", kv[1]));
}

void TYsonDict::SetStruct(size_t ind, IStructConstPtr value) {
    const auto& kv = std::dynamic_pointer_cast<const TYsonStruct>(value)->Underlying();
    Underlying()[ind] = TNode().Add(kv["key"]).Add(kv["value"]);
}

void TYsonDict::SetStruct(size_t ind, IStructPtr&& value) {
    auto kv = std::dynamic_pointer_cast<TYsonStruct>(value)->Release();
    Underlying()[ind] = TNode().Add(std::move(kv["key"])).Add(std::move(kv["value"]));
}

// TYsonRow

TYsonRow::TYsonRow() : TYsonStruct(NTi::Dict(NTi::Void(), NTi::Void())) { }

TYsonRow::TYsonRow(NTi::TTypePtr schema) : TYsonStruct(schema) { }

TYsonRow::TYsonRow(NTi::TTypePtr schema, TNode underlying)
  : TYsonStruct(schema, std::move(underlying)) { }

TYsonRow::TYsonRow(const NYT::TTableSchema& schema) : TYsonRow(TableSchemaToStructType(schema)) { }

TYsonRow::TYsonRow(const NYT::TTableSchema& schema, TNode underlying)
  : TYsonRow(TableSchemaToStructType(schema), std::move(underlying)) { }

TYsonRow::TYsonRow(const TYsonRow& rhs) : TYsonStruct(rhs) { }

TYsonRow::TYsonRow(TYsonRow&& rhs) : TYsonStruct(static_cast<TYsonStruct&&>(rhs)) { }
    
TYsonRow& TYsonRow::operator=(const TYsonRow& rhs) {
    TYsonStruct::operator=(rhs);
    return *this;
}

TYsonRow& TYsonRow::operator=(TYsonRow&& rhs) {
    TYsonStruct::operator=(static_cast<TYsonStruct&&>(rhs));
    return *this;
}

IRowPtr TYsonRow::CopyRow() const {
    return std::make_shared<TYsonRow>(*this); 
}

NTi::TTypePtr TYsonRow::GetSchema(size_t ind) const {
    return GetSchema()->AsStruct()->GetMembers()[ind].GetType();
}

const TNode& TYsonRow::GetData(size_t ind) const {
    return GetData(GetSchema()->AsStruct()->GetMembers()[ind].GetName());
}

TNode& TYsonRow::GetData(size_t ind) {
    return GetData(GetSchema()->AsStruct()->GetMembers()[ind].GetName());
}

}
