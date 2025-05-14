#pragma once

#include <variant>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <dformats/yson/yson_types.h>
#include <dformats/common/util.h>

namespace DFormats {

using TArrowStruct = TYsonStruct;
using TArrowTuple = TYsonTuple;
using TArrowList = TYsonList;
using TArrowDict = TYsonDict;
using TArrowVariant = TYsonVariant;
using TArrowOptional = TYsonOptional;

class TArrowRow : public TYsonRow {
public:
    TArrowRow() = default;
    TArrowRow(NTi::TTypePtr schema);
    TArrowRow(NTi::TTypePtr schema, TNode underlying);
    TArrowRow(const NYT::TTableSchema& schema);
    TArrowRow(const NYT::TTableSchema& schema, TNode underlying);

    TArrowRow(const TArrowRow& rhs);
    TArrowRow(TArrowRow&& rhs);
    
    TArrowRow& operator=(const TArrowRow& rhs);
    TArrowRow& operator=(TArrowRow&& rhs);

    IRowPtr CopyRow() const override;

    void SerializeComplexNodes();

    inline size_t Size() const {
        return Underlying().Size();
    } 

    using TYsonRow::GetSchema;

protected:
    using TYsonRow::GetData;

    IStructConstPtr GetStruct(size_t ind) const override;
    ITupleConstPtr GetTuple(size_t ind) const override;
    IListConstPtr GetList(size_t ind) const override;
    IDictConstPtr GetDict(size_t ind) const override;
    IVariantConstPtr GetVariant(size_t ind) const override;
    IOptionalConstPtr GetOptional(size_t ind) const override;

    IStructConstPtr GetStruct(std::string_view ind) const override;
    ITupleConstPtr GetTuple(std::string_view ind) const override;
    IListConstPtr GetList(std::string_view ind) const override;
    IDictConstPtr GetDict(std::string_view ind) const override;
    IVariantConstPtr GetVariant(std::string_view ind) const override;
    IOptionalConstPtr GetOptional(std::string_view ind) const override;
};

}
