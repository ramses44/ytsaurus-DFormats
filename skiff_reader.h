#pragma once

#include <yt/cpp/mapreduce/interface/skiff_row.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/client/skiff.h>
#include <yt/cpp/mapreduce/io/lenval_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>
#include <yt/yt/client/tablet_client/public.h>

#include <iostream>

#include "skiff_schema.h"
#include "skiff_types.h"
#include "reading_context.h"

using namespace NYT;

class TSkiffRowReader {  // TODO: посмотреть интерфейс (тут)[/home/ramses44/fqw/ytsaurus/yt/cpp/mapreduce/interface/io.h:237]
public:
    using TOptions = NYT::NDetail::TCreateSkiffSchemaOptions;

public:
    TSkiffRowReader(IInputStream* underlying,
                    TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas,
                    TOptions options = TOptions());
    TSkiffRowReader(IInputStream* underlying,
                    const TVector<TTableSchema>& tableSchemas,
                    TOptions options = TOptions());

    TSkiffRowReader(TSkiffRowReader&& rhs) noexcept = default;

    TSkiffRowReader& operator=(TSkiffRowReader&& rhs) noexcept = default;

    std::unique_ptr<TSkiffRow> ReadRow();
    void SkipRow();
    
    bool IsValid();  // По уму должен быть const, но мы в нём читаем из потока
    void Next();
    uint16_t GetTableIndex() const;
    int64_t GetRangeIndex() const;
    int64_t GetRowIndex() const;
    bool IsAfterKeySwitch() const;
    const TReadingContext& GetReadingContext() const;

protected:
    template <class T>
    bool ReadFromStream(T* dst, size_t len = sizeof(T), bool allowEOS = false);
    bool SkipFromStream(size_t len, bool allowEOS = false);
    void ReadField(const NSkiff::TSkiffSchemaPtr fieldSkiffSchema, size_t nestedLevel = 0);
    void SkipField(const NSkiff::TSkiffSchemaPtr fieldSkiffSchema);
    void ReadContext();
    void ClearContext();

protected:
    IInputStream* Underlying_;  // TODO: smart pointers
    NSkiff::TSkiffSchemaPtr SkiffSchema_;
    const TOptions Options_;
    std::unique_ptr<TSkiffRow> Row_;
    TReadingContext ReadingContext_;
    mutable TMaybe<uint16_t> NextTableIndex_;
};