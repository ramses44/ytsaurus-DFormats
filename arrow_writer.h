#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "arrow_adapter.h"
#include "arrow_types.h"

using namespace arrow;

class TArrowRowWriter {
public:
    TArrowRowWriter(
      TVector<std::shared_ptr<io::OutputStream>> outputs,
      TVector<TArrowSchemaPtr> arrowSchemas,
      TVector<size_t> batchSizes = {});

    TArrowRowWriter(TVector<IOutputStream*> outputs,
      TVector<TArrowSchemaPtr> arrowSchemas, TVector<size_t> batchSizes = {});

    TArrowRowWriter& operator=(TArrowRowWriter&& rhs) noexcept = default;

    void Finish(size_t tableIndex);

    void AddRow(std::unique_ptr<TArrowRow> row, size_t tableIndex);

    TArrowSchemaPtr ArrowSchema(size_t tableIndex) const {
        return ArrowSchemas_[tableIndex];
    }

protected:
    void InitArrayBuilders();
    void WriteBatch(size_t tableIndex);

protected:
    TVector<size_t> BatchSizes_;
    TVector<std::shared_ptr<ipc::RecordBatchWriter>> Underlying_;
    TVector<std::shared_ptr<Schema>> ArrowSchemas_;
    TVector<TVector<std::shared_ptr<ArrayBuilder>>> ArrayBuilders_;
};
