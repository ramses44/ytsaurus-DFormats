#pragma once

#include <yt/cpp/mapreduce/io/proto_table_writer.h>

#include "protobuf_row_factory.h"
#include <dformats/interface/io.h>

using namespace google::protobuf;
using namespace NYT;

namespace DFormats {

class TProtobufRowWriter : public IRowWriter {
public:
    TProtobufRowWriter(THolder<IProxyOutput> output, std::shared_ptr<TProtobufRowFactory> rowFactory,
                    std::vector<std::string> typeNames);
    TProtobufRowWriter(THolder<IProxyOutput> output, std::shared_ptr<TProtobufRowFactory> rowFactory,
                    const std::vector<size_t>& outputTypesNumbers);

    TProtobufRowWriter(TProtobufRowWriter&& rhs) = default;
    TProtobufRowWriter& operator=(TProtobufRowWriter&& rhs) = default;

    void WriteRow(const IRowConstPtr& row, size_t tableIndex) override;
    void WriteRow(IRowPtr&& row, size_t tableIndex) override;
    void FinishTable(size_t tableIndex) override;

    enum Format Format() const override;
    size_t GetTablesCount() const override;
    const NYT::TTableSchema& GetTableSchema(size_t tableIndex) const override;
    IRowPtr CreateObjectForWrite(size_t tableIndex) const override;

    std::shared_ptr<TProtobufRowFactory> RowFactory() const;

protected:
    std::unique_ptr<TLenvalProtoTableWriter> Underlying_;
    std::shared_ptr<TProtobufRowFactory> RowFactory_;
    std::vector<std::string> TypeNames_;
};

}
