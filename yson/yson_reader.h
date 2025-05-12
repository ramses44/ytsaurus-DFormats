#pragma once

#include <yt/cpp/mapreduce/io/node_table_reader.h>

#include "yson_types.h"
#include <dformats/interface/io.h>

using namespace NYT;

namespace DFormats {

class TYsonRowReader : public IRowReader {
public:
    TYsonRowReader(::TIntrusivePtr<TRawTableReader> input, std::vector<TTableSchema> schemas);

    TYsonRowReader(TYsonRowReader&& rhs) = default;
    TYsonRowReader& operator=(TYsonRowReader&& rhs) = default;

    IRowPtr ReadRow() override;

    bool IsValid() const override;
    bool IsEndOfStream() const override;
    void Next() override;

    const TReadingContext& GetReadingContext() const override;
    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;

private:
    void RefreshReadingContext();

private:
    std::unique_ptr<TNodeTableReader> Underlying_;
    std::vector<TTableSchema> TableSchemas_;
    TReadingContext ReadingContext_;
};

}
