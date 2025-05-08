#include "skiff_writer.h"

namespace DFormats {

TSkiffRowWriter::TSkiffRowWriter(THolder<IProxyOutput> output, std::vector<NYT::TTableSchema> schemas)
  : Underlying_(std::move(output)), TableSchemas_(std::move(schemas)) { }

void TSkiffRowWriter::WriteRow(const IRowConstPtr& row, size_t tableIndex) {
    auto* stream = Underlying_->GetStream(tableIndex);
    auto serialization = std::dynamic_pointer_cast<const TSkiffRow>(row)->Serialize();

    stream->Write(&tableIndex, 2);
    stream->Write(serialization.Data(), serialization.Size());

    Underlying_->OnRowFinished(tableIndex);
}

void TSkiffRowWriter::WriteRow(IRowPtr&& row, size_t tableIndex) {
    auto* stream = Underlying_->GetStream(tableIndex);
    auto serialization = std::move(*std::dynamic_pointer_cast<TSkiffRow>(row)).Serialize();

    stream->Write(&tableIndex, 2);
    stream->Write(serialization.Data(), serialization.Size());

    Underlying_->OnRowFinished(tableIndex);
}

void TSkiffRowWriter::FinishTable(size_t tableIndex) {
    Underlying_->GetStream(tableIndex)->Finish();
}

Format TSkiffRowWriter::Format() const {
    return Format::Skiff;
}

size_t TSkiffRowWriter::GetTablesCount() const {
    return Underlying_->GetStreamCount();
}

const NYT::TTableSchema& TSkiffRowWriter::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

IRowPtr TSkiffRowWriter::CreateObjectForWrite(size_t tableIndex) const {
    return std::make_shared<TSkiffRow>(GetTableSchema(tableIndex));
}

}
