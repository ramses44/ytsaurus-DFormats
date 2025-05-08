#pragma once

#include <yt/cpp/mapreduce/interface/skiff_row.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/client/skiff.h>
#include <yt/cpp/mapreduce/io/lenval_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>
#include <yt/yt/client/tablet_client/public.h>

#include "skiff_schema.h"
#include "skiff_types.h"
#include <dformats/interface/io.h>

namespace DFormats {

class TSkiffRowReader : public IRowReader {
public:
    TSkiffRowReader(::TIntrusivePtr<TRawTableReader> underlying, std::vector<TTableSchema> schemas,
        ReadingOptions options = static_cast<ReadingOptions>(0));

    TSkiffRowReader(TSkiffRowReader&& rhs) = default;
    TSkiffRowReader& operator=(TSkiffRowReader&& rhs) = default;

    IRowPtr ReadRow() override;
    
    bool IsValid() const override;
    bool IsEndOfStream() const override;
    void Next() override;

    const TReadingContext& GetReadingContext() const override;
    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;

protected:
    template <class T>
    bool ReadFromStream(T* dst, size_t len = sizeof(T), bool allowEOS = false);
    bool SkipFromStream(size_t len, bool allowEOS = false);
    size_t ReadData(NSkiff::TSkiffSchemaPtr skiffSchema, TBuffer& dst);
    void SkipData(NSkiff::TSkiffSchemaPtr skiffSchema);
    void ReadContext();

    NSkiff::TSkiffSchemaPtr GetSkiffSchema(size_t tableIndex) const;
    void CalculateUnitable();

private:
    ::TIntrusivePtr<TRawTableReader> Underlying_;
    std::vector<TTableSchema> TableSchemas_;
    std::vector<NSkiff::TSkiffSchemaPtr> SkiffSchemas_;
    const ReadingOptions ReadingOptions_;

    TReadingContext ReadingContext_;
    bool Valid_ = false;
    bool EndOfStream_ = false;

    // For reading optimization. Contains how many bytes may be read unitedly from stream
    TVector<TVector<size_t>> Unitable_;
};

}
