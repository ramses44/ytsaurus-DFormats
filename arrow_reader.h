#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <iostream>
#include <string>

#include "arrow_types.h"
#include "reading_context.h"

using namespace arrow;

void PrintTypes(TArrowSchemaPtr schema);
TArrowSchemaPtr RemoveReadingContextColumns(TArrowSchemaPtr schema);

class TArrowRowReader {
public:
    TArrowRowReader(std::shared_ptr<io::InputStream> underlying);
    TArrowRowReader(IInputStream* input);
    TArrowRowReader(TArrowRowReader&& rhs) noexcept = default;

    TArrowRowReader& operator=(TArrowRowReader&& rhs) noexcept = default;

    std::unique_ptr<TArrowRow> ReadRow();
    void SkipRow();
    
    bool IsValid() const;
    void Next();
    uint16_t GetTableIndex() const;
    int64_t GetRangeIndex() const;
    int64_t GetRowIndex() const;
    bool IsAfterKeySwitch() const;
    TReadingContext GetReadingContext() const;

    TArrowSchemaPtr ArrowSchema() const;

protected:
    std::shared_ptr<ipc::RecordBatchStreamReader> Underlying_;
    std::shared_ptr<RecordBatch> CurrentBatch_;
    int CurrentBatchRowId_; 
};
