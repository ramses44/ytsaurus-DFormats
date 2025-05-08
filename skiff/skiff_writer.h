#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/client/skiff.h>

#include "skiff_schema.h"
#include "skiff_types.h"
#include <dformats/interface/io.h>

using namespace NYT;

namespace DFormats {

class TSkiffRowWriter : public IRowWriter {
public:
    TSkiffRowWriter(THolder<IProxyOutput> output, std::vector<NYT::TTableSchema> schemas);

    void WriteRow(const IRowConstPtr& row, size_t tableIndex) override;
    void WriteRow(IRowPtr&& row, size_t tableIndex) override;
    void FinishTable(size_t tableIndex) override;

    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;
    IRowPtr CreateObjectForWrite(size_t tableIndex) const override;

private:
    THolder<IProxyOutput> Underlying_;
    std::vector<NYT::TTableSchema> TableSchemas_;
};

}
