#include <util/stream/output.h>
#include <util/system/user.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <iostream>
#include <map>

#include <dformats/skiff/skiff_reader.h>
#include <dformats/skiff/skiff_writer.h>
#include <dformats/protobuf/protobuf_reader.h>
#include <dformats/protobuf/protobuf_writer.h>
#include <dformats/yson/yson_reader.h>
#include <dformats/yson/yson_writer.h>
#include <dformats/arrow/arrow_reader.h>
#include <dformats/arrow/arrow_writer.h>

#include <dformats/interface/mapreduce.h>

#include <dformats/benchmarks/bench.pb.h>

using namespace NYT;
using namespace DFormats;

namespace b6 {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* /* reader */, TWriter* writer) override {
        auto row = TNode()
            ("column_1", -234234324)
            ("column_2", true)
            ("column_3", -42)
            ("column_4", 3.1415926)
            ("column_5", "Hello world!")
            ("column_6", 45423456)
            ("column_7", 34567)
            ("column_8", "YTsaurus")
            ("column_9", false)
            ("column_10", 42);
        for (size_t i = 0; i < 5'000'000; ++i) {
            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperStaticProto
    : public IMapper<TTableReader<TSimpleTenMessage>, TTableWriter<TSimpleTenMessage>>
{
public:
    void Do(TReader* /* reader */, TWriter* writer) override {
        TSimpleTenMessage row;
        row.set_column_1(-234234324);
        row.set_column_2(true);
        row.set_column_3(-42);
        row.set_column_4(3.1415926);
        row.set_column_5("Hello world!");
        row.set_column_6(45423456);
        row.set_column_7(34567);
        row.set_column_8("YTsaurus");
        row.set_column_9(false);
        row.set_column_10(42);

        for (size_t i = 0; i < 5'000'000; ++i) {
            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperStaticProto);

class TBenchmarkMapper : public TJob {
public:
    template <typename... Args>
    TBenchmarkMapper(Args&&... args) : TJob(std::forward<Args>(args)...) { }

    void DoImpl(IRowReader* /* reader */, IRowWriter* writer) {
        auto row = writer->CreateObjectForWrite(0);

        row->SetValue<int64_t>(0, -234234324);
        row->SetValue<bool>(1, true);
        row->SetValue<int32_t>(2, -42);
        row->SetValue<double>(3, 3.1415926);
        row->SetValue<std::string>(4, "Hello world!");
        row->SetValue<uint32_t>(5, 45423456);
        row->SetValue<uint64_t>(6, 34567);
        row->SetValue<std::string>(7, "YTsaurus");
        row->SetValue<bool>(8, false);
        row->SetValue<uint32_t>(9, 42);

        for (size_t i = 0; i < 5'000'000; ++i) {
            writer->WriteRow(row, 0);
        }

        writer->FinishTable(0);
    }
};
REGISTER_RAW_JOB(TBenchmarkMapper);

}

int main(int argc, char** argv) {
    Initialize(argc, argv);

    Y_ENSURE(argc > 1, "Missing `format` argument!");
    TString format = argv[1];
    int jobCount = argc > 2 ? FromString<int>(argv[2]) : 14;

    auto client = CreateClient("127.0.0.1:8000");
    TString inputTable = "//home/simple_ten";
    TString outputTable = "//tmp/out_" + format;

    TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));
    
    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", inputTableSchema.ToNode())));

    MapReduceIOSchema ioSchema = {
        {inputTableSchema},
        Format::Yson,
        {0},
        Format::Yson,
        {0}
    };

    if (format == "classic-yson") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new b6::TBenchmarkMapperYson);
    } else if (format == "static-protobuf") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TSimpleTenMessage>(inputTable)
                .AddOutput<TSimpleTenMessage>(outputTable),
            new b6::TBenchmarkMapperStaticProto);
    } else {
        if (format == "dynamic-protobuf") {
            ioSchema.InputFormat = ioSchema.OutputFormat = Format::Protobuf;
        } else if (format == "skiff") {
            ioSchema.InputFormat = ioSchema.OutputFormat = Format::Skiff;
        } else if (format == "arrow") {
            ioSchema.InputFormat = ioSchema.OutputFormat = Format::Arrow;
        } else if (format == "yson") {
            ioSchema.InputFormat = ioSchema.OutputFormat = Format::Yson;
        } else {
            ythrow yexception() << "Unknown format \"" << format << 
                "\". It must be yson/classic-yson/skiff/static-protobuf/dynamic-protobuf/arrow.";
        }

        auto ioFormats = MakeIOFormats(ioSchema);

        client->RawMap(
            TRawMapOperationSpec()
                .JobCount(jobCount)
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(ioFormats.first)
                .OutputFormat(ioFormats.second),
            new b6::TBenchmarkMapper(std::move(ioSchema)));
    }

    return 0;
}