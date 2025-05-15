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

namespace b2 {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        size_t totalSize = 0;
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            totalSize += 8;
            totalSize += row["data"].AsString().size();
        }

        writer->AddRow(TNode()("data", TNode()("total_size", totalSize)));
        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperStaticProto
    : public IMapper<TTableReader<TSimpleTwoMessage>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        size_t totalSize = 0;
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            totalSize += 8;
            totalSize += row.data().size();
        }

        writer->AddRow(TNode()("data", TNode()("total_size", totalSize)));
        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperStaticProto);

class TBenchmarkMapper : public TJob {
public:
    template <typename... Args>
    TBenchmarkMapper(Args&&... args) : TJob(std::forward<Args>(args)...) { }

    void DoImpl(IRowReader* reader, IRowWriter* writer) {
        size_t totalSize = 0;
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->ReadRow();
            
            totalSize += 8;
            totalSize += row->GetValue<std::string_view>(1).size();

            writer->WriteRow(std::move(row), 0);
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
    TString inputTable = "//home/simple_two";
    TString outputTable = "//tmp/out_" + format;

    [[maybe_unused]] TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));

    auto outputSchemaNode = TNode()
        .Add(TNode()("name", "data")("type", "any"));
    
    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", outputSchemaNode)));

    MapReduceIOSchema ioSchema = {
        {inputTableSchema, TTableSchema::FromNode(outputSchemaNode)},
        Format::Yson,  // For default
        {0},
        Format::Yson,
        {1}
    };

    if (format == "classic-yson") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new b2::TBenchmarkMapperYson);
    } else if (format == "static-protobuf") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TSimpleTwoMessage>(inputTable)
                .AddOutput<TNode>(outputTable),
            new b2::TBenchmarkMapperStaticProto);
    } else {
        if (format == "dynamic-protobuf") {
            ioSchema.InputFormat = Format::Protobuf;
        } else if (format == "skiff") {
            ioSchema.InputFormat = Format::Skiff;
        } else if (format == "arrow") {
            ioSchema.InputFormat = Format::Arrow;
        } else if (format == "yson") {
            ioSchema.InputFormat = Format::Yson;
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
            new b2::TBenchmarkMapper(std::move(ioSchema)));
    }

    return 0;
}