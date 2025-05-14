#include "arrow_types.h"

#include <library/cpp/yson/node/node_io.h>

namespace DFormats {

TArrowRow::TArrowRow(NTi::TTypePtr schema) : TYsonStruct(schema) { }

TArrowRow::TArrowRow(NTi::TTypePtr schema, TNode underlying)
  : TYsonStruct(schema, std::move(underlying)) { }

TArrowRow::TArrowRow(const NYT::TTableSchema& schema)
  : TYsonStruct(TableSchemaToStructType(schema)) { }

TArrowRow::TArrowRow(const NYT::TTableSchema& schema, TNode underlying)
  : TYsonStruct(TableSchemaToStructType(schema), std::move(underlying)) { }

TArrowRow::TArrowRow(const TArrowRow& rhs) : TYsonStruct(rhs) { }

TArrowRow::TArrowRow(TArrowRow&& rhs) : TYsonStruct(std::move(rhs)) { }
    
TArrowRow& TArrowRow::operator=(const TArrowRow& rhs) {
    TYsonRow::operator=(rhs);
    return *this;
}

TArrowRow& TArrowRow::operator=(TArrowRow&& rhs) {
    TYsonRow::operator=(std::move(rhs));
    return *this;
}

IRowPtr TArrowRow::CopyRow() const {
    return std::make_shared<TArrowRow>(*this);
}

void TArrowRow::SerializeComplexNodes() {
    for (auto& [key, value] : this->Underlying().AsMap()) {
        if (value.IsList() || value.IsMap()) {
            value = TNode(NYT::NodeToYsonString(value));
        }
    }
}

IStructConstPtr TArrowRow::GetStruct(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowStruct>(GetSchema(ind), std::move(dataNode));
}
ITupleConstPtr TArrowRow::GetTuple(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowTuple>(GetSchema(ind), std::move(dataNode));
}
IListConstPtr TArrowRow::GetList(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowList>(GetSchema(ind), std::move(dataNode));
}
IDictConstPtr TArrowRow::GetDict(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowDict>(GetSchema(ind), std::move(dataNode));
}
IVariantConstPtr TArrowRow::GetVariant(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowVariant>(GetSchema(ind), std::move(dataNode));
}
IOptionalConstPtr TArrowRow::GetOptional(size_t ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowOptional>(GetSchema(ind), std::move(dataNode));
}
    
IStructConstPtr TArrowRow::GetStruct(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowStruct>(GetSchema(ind), std::move(dataNode));
}
ITupleConstPtr TArrowRow::GetTuple(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowTuple>(GetSchema(ind), std::move(dataNode));
}
IListConstPtr TArrowRow::GetList(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowList>(GetSchema(ind), std::move(dataNode));
}
IDictConstPtr TArrowRow::GetDict(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowDict>(GetSchema(ind), std::move(dataNode));
}
IVariantConstPtr TArrowRow::GetVariant(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowVariant>(GetSchema(ind), std::move(dataNode));
}
IOptionalConstPtr TArrowRow::GetOptional(std::string_view ind) const {
    TNode dataNode = GetData(ind);
    if (dataNode.IsString()) {
        dataNode = NYT::NodeFromYsonString(dataNode.AsString());
    }
    return std::make_shared<TArrowOptional>(GetSchema(ind), std::move(dataNode));
}

}
