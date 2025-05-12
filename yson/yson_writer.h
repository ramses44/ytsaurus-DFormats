#pragma once

#include <yt/cpp/mapreduce/io/node_table_writer.h>

#include "yson_types.h"
#include <dformats/interface/io.h>

using namespace NYT;

namespace DFormats {

class TYsonRowWriter : public IRowWriter {
public:
    TYsonRowWriter(THolder<IProxyOutput> output, std::vector<TTableSchema> schemas);

    TYsonRowWriter(TYsonRowWriter&& rhs) = default;
    TYsonRowWriter& operator=(TYsonRowWriter&& rhs) = default;

    void WriteRow(const IRowConstPtr& row, size_t tableIndex) override;
    void WriteRow(IRowPtr&& row, size_t tableIndex) override;
    void FinishTable(size_t tableIndex) override;

    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;
    IRowPtr CreateObjectForWrite(size_t tableIndex) const override;

protected:
    std::unique_ptr<TNodeTableWriter> Underlying_;
    std::vector<TTableSchema> TableSchemas_;
};

}
