#include <yt/cpp/mapreduce/interface/client.h>
#include <util/stream/output.h>
#include <util/system/user.h>
#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/skiff_row_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <iostream>
#include <map>

#include <dformats/skiff_reader.h>
#include <dformats/skiff_writer.h>
#include <dformats/arrow_reader.h>
#include <dformats/arrow_writer.h>
#include <dformats/benchmarks/bench.pb.h>

using namespace NYT;

namespace SimpleTwo {

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

class TBenchmarkMapperProto
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
REGISTER_MAPPER(TBenchmarkMapperProto);

class TBenchmarkMapperSkiff : public IRawJob {
public:
    Y_SAVELOAD_JOB(InputTableSchemaNode_);

    TBenchmarkMapperSkiff() = default;
    TBenchmarkMapperSkiff(const TTableSchema& inputTableSchema) 
        : InputTableSchemaNode_(inputTableSchema.ToNode()) {}

    void Do(const TRawJobContext& context) override {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TBufferedInput input(&unbufferedInput);
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[0]);
        TBufferedOutput output(&unbufferedOutput);
        
        TSkiffRowReader reader(&input, {TTableSchema::FromNode(InputTableSchemaNode_)});
        TNodeTableWriter writer(THolder<IProxyOutput>(MakeHolder<TJobWriter>(context.GetOutputFileList())));

        size_t totalSize = 0;
        for (size_t i = 0; reader.IsValid(); reader.Next(), ++i) {
            auto row = reader.ReadRow();

            totalSize += 8;
            totalSize += row->GetRaw(1).size();
        }

        writer.AddRow(TNode()("data", TNode()("total_size", totalSize)), 0);
        writer.FinishTable(0);
    }

private:
    TNode InputTableSchemaNode_;
};
REGISTER_RAW_JOB(TBenchmarkMapperSkiff);

class TBenchmarkMapperArrow : public IRawJob {
public:
    TBenchmarkMapperArrow() = default;
    
    void Do(const TRawJobContext& context) override {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TBufferedInput input(&unbufferedInput);
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[0]);
        TBufferedOutput output(&unbufferedOutput);
            
        TArrowRowReader reader(&input);
        TNodeTableWriter writer(THolder<IProxyOutput>(MakeHolder<TJobWriter>(context.GetOutputFileList())));

        size_t totalSize = 0;
        for (; reader.IsValid(); reader.Next()) {
            auto row = reader.ReadRow();

            totalSize += 8;
            totalSize += row->GetData<std::string>(1).size();
        }
        
        writer.AddRow(TNode()("data", TNode()("total_size", totalSize)), 0);
        writer.FinishTable(0);
    }
};
REGISTER_RAW_JOB(TBenchmarkMapperArrow);

}

int main(int argc, char** argv) {
    Initialize(argc, argv);

    Y_ENSURE(argc > 1, "Missing `format` argument!");
    TString format = argv[1];

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

    if (format == "yson") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new SimpleTwo::TBenchmarkMapperYson);
    } else if (format == "protobuf") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TSimpleTwoMessage>(inputTable)
                .AddOutput<TNode>(outputTable),
            new SimpleTwo::TBenchmarkMapperProto);
    } else if (format == "skiff") {
        auto skiffTableSchema = SkiffSchemaFromTableSchema(inputTableSchema);
    
        auto skiffOptions = NYT::NDetail::TCreateSkiffSchemaOptions();
        auto inputTableSkiffSchema = NYT::NDetail::CreateSkiffSchema({skiffTableSchema}, skiffOptions);
    
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(inputTableSkiffSchema)))
                .OutputFormat(TFormat("yson")),
            new SimpleTwo::TBenchmarkMapperSkiff(inputTableSchema));
    } else if (format == "arrow") {
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat("arrow"))
                .OutputFormat(TFormat("yson")),
            new SimpleTwo::TBenchmarkMapperArrow);
    } else {
        ythrow yexception() << "Unknown format \"" + format + "\". It should be yson/skiff/protobuf.";
    }

    return 0;
}