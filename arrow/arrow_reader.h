#pragma once

#include <string>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include "arrow_types.h"
#include "arrow_schema.h"
#include <dformats/interface/io.h>

using namespace arrow;

namespace DFormats {

class TArrowRowReader : public IRowReader {
public:
    TArrowRowReader(::TIntrusivePtr<TRawTableReader> input, std::vector<NYT::TTableSchema> schemas);

    TArrowRowReader(TArrowRowReader&& rhs) = default;
    TArrowRowReader& operator=(TArrowRowReader&& rhs) = default;

    IRowPtr ReadRow() override;

    bool IsValid() const override;
    bool IsEndOfStream() const override;
    void Next() override;

    const TReadingContext& GetReadingContext() const override;
    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;

    TArrowSchemaPtr GetArrowSchema() const;

private:
    ::TIntrusivePtr<TRawTableReader> Underlying_;
    std::shared_ptr<ipc::RecordBatchStreamReader> ArrowStream_;
    std::shared_ptr<RecordBatch> CurrentBatch_;
    std::vector<NYT::TTableSchema> TableSchemas_;
    TReadingContext ReadingContext_;
    int CurrentBatchRowId_; 
};

}
