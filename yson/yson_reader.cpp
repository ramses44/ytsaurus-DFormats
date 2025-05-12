#include "yson_reader.h"

namespace DFormats {

TYsonRowReader::TYsonRowReader(::TIntrusivePtr<TRawTableReader> input, std::vector<TTableSchema> schemas)
  : Underlying_(new TNodeTableReader(std::move(input))), TableSchemas_(std::move(schemas)) {
    RefreshReadingContext();
}

bool TYsonRowReader::IsValid() const {
    return Underlying_->IsValid();
}

bool TYsonRowReader::IsEndOfStream() const {
    return Underlying_->IsEndOfStream();
}

void TYsonRowReader::Next() {
    Underlying_->Next();
    RefreshReadingContext();
}

IRowPtr TYsonRowReader::ReadRow()  {
    TNode node;
    Underlying_->MoveRow(&node);
    return std::make_shared<TYsonRow>(TableSchemas_[ReadingContext_.TableIndex], std::move(node));
}

const TReadingContext& TYsonRowReader::GetReadingContext() const {
    return ReadingContext_;
}

Format TYsonRowReader::Format() const {
    return Format::Yson;
}

size_t TYsonRowReader::GetTablesCount() const {
    return TableSchemas_.size();
}

const NYT::TTableSchema& TYsonRowReader::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

void TYsonRowReader::RefreshReadingContext() {
    if (Underlying_->IsValid())
        ReadingContext_ = {Underlying_->GetTableIndex(), Underlying_->GetRowIndex(), Underlying_->GetRangeIndex(), {}};
    else
        ReadingContext_ = {};
}

}
