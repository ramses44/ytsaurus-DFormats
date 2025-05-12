#include "yson_writer.h"

namespace DFormats {

TYsonRowWriter::TYsonRowWriter(THolder<IProxyOutput> output, std::vector<TTableSchema> schemas)
  : Underlying_(new TNodeTableWriter(std::move(output))), TableSchemas_(std::move(schemas)) { }

void TYsonRowWriter::WriteRow(const IRowConstPtr& row, size_t tableIndex) {
    Underlying_->AddRow(std::dynamic_pointer_cast<const TYsonRow>(row)->Underlying(), tableIndex);
}

void TYsonRowWriter::WriteRow(IRowPtr&& row, size_t tableIndex) {
    Underlying_->AddRow(std::dynamic_pointer_cast<TYsonRow>(row)->Release(), tableIndex);
}

void TYsonRowWriter::FinishTable(size_t tableIndex) {
    Underlying_->FinishTable(tableIndex);
}

Format TYsonRowWriter::Format() const {
    return Format::Yson;
}

size_t TYsonRowWriter::GetTablesCount() const {
    return Underlying_->GetTableCount();
}

const NYT::TTableSchema& TYsonRowWriter::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

IRowPtr TYsonRowWriter::CreateObjectForWrite(size_t tableIndex) const {
    return std::make_shared<TYsonRow>(GetTableSchema(tableIndex));
}

}
