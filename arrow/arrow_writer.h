#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "arrow_adapter.h"
#include "arrow_types.h"
#include "arrow_schema.h"
#include <dformats/interface/io.h>

using namespace arrow;

namespace DFormats {

class TArrowRowWriter : public IRowWriter {
public:
    size_t kDefaultBatchSize = 1000;

public:
    TArrowRowWriter(THolder<IProxyOutput> output, 
        std::vector<NYT::TTableSchema> schemas, std::vector<size_t> batchSizes = {});

    TArrowRowWriter(TArrowRowWriter&& rhs) = default;
    TArrowRowWriter& operator=(TArrowRowWriter&& rhs) = default;

    void WriteRow(const IRowConstPtr& row, size_t tableIndex) override;
    void WriteRow(IRowPtr&& row, size_t tableIndex) override;
    void FinishTable(size_t tableIndex) override;

    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;
    IRowPtr CreateObjectForWrite(size_t tableIndex) const override;

    TArrowSchemaPtr GetArrowSchema(size_t tableIndex) const;

protected:
    void InitArrayBuilders();
    void WriteBatch(size_t tableIndex);

protected:
    THolder<IProxyOutput> Underlying_;
    std::vector<size_t> BatchSizes_;
    std::vector<std::shared_ptr<ipc::RecordBatchWriter>> ArrowStreams_;
    std::vector<NYT::TTableSchema> TableSchemas_;
    std::vector<TArrowSchemaPtr> ArrowSchemas_;
    std::vector<std::vector<std::shared_ptr<ArrayBuilder>>> ArrayBuilders_;
};

}
