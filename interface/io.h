#pragma once

#include <optional>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include "types.h"

namespace DFormats {

enum Format {
    Yson,
    Skiff,
    Protobuf,
    Arrow
};

enum ReadingOptions {
    HasRowIndex = 1,
    HasRangeIndex = 2,
    HasAfterKeySwitch = 4
};

struct TReadingContext {
    size_t TableIndex = 0;
    std::optional<size_t> RowIndex;
    std::optional<size_t> RangeIndex;
    std::optional<bool> AfterKeySwitch;
};

class IRowReader {
public:
    virtual ~IRowReader() {}

    virtual IRowPtr ReadRow() = 0;
    virtual void Next() = 0;
    virtual bool IsValid() const = 0;
    virtual bool IsEndOfStream() const = 0;

    inline size_t GetTableIndex() const {
        return GetReadingContext().TableIndex;
    }
    inline size_t GetRangeIndex() const {
        return GetReadingContext().RowIndex.value_or(0);
    }
    inline size_t GetRowIndex() const {
        return GetReadingContext().RangeIndex.value_or(0);
    }
    inline bool IsAfterKeySwitch() const {
        return GetReadingContext().AfterKeySwitch.value_or(false);
    }

    virtual const TReadingContext& GetReadingContext() const = 0;
    virtual enum Format Format() const = 0;
    virtual size_t GetTablesCount() const = 0;
    virtual const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const = 0;
};

class IRowWriter {
public:
    virtual ~IRowWriter() {}

    virtual void WriteRow(const IRowConstPtr& row, size_t tableIndex) = 0;
    virtual void WriteRow(IRowPtr&& row, size_t tableIndex) = 0;
    virtual void FinishTable(size_t tableIndex) = 0;

    virtual enum Format Format() const = 0;
    virtual size_t GetTablesCount() const = 0;
    virtual const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const = 0;
    virtual IRowPtr CreateObjectForWrite(size_t tableIndex) const = 0;
};

}