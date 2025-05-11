#pragma once

#include <yt/cpp/mapreduce/io/proto_table_reader.h>

#include "protobuf_row_factory.h"
#include <dformats/interface/io.h>

using namespace google::protobuf;
using namespace NYT;

namespace DFormats {

class TProtobufRowReader : public IRowReader {
public:
    TProtobufRowReader(::TIntrusivePtr<TRawTableReader> input,
        std::shared_ptr<TProtobufRowFactory> rowFactory, std::vector<std::string> typesNames);
    TProtobufRowReader(::TIntrusivePtr<TRawTableReader> input,
        std::shared_ptr<TProtobufRowFactory> rowFactory, const std::vector<size_t>& inputTypesNumbers);

    TProtobufRowReader(TProtobufRowReader&& rhs) = default;
    TProtobufRowReader& operator=(TProtobufRowReader&& rhs) = default;

    std::unique_ptr<Message> ReadRawMessage();
    IRowPtr ReadRow() override;

    bool IsValid() const override;
    bool IsEndOfStream() const override;
    void Next() override;

    const TReadingContext& GetReadingContext() const override;
    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;

    std::shared_ptr<TProtobufRowFactory> RowFactory() const;

private:
    void RefreshReadingContext();

private:
    std::unique_ptr<TLenvalProtoTableReader> Underlying_;
    std::shared_ptr<TProtobufRowFactory> RowFactory_;
    std::vector<std::string> TypeNames_;
    TReadingContext ReadingContext_;
};

}
