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

namespace ComplexTypes {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperSkiff : public IRawJob {
public:
    Y_SAVELOAD_JOB(TableSchemaNode_);

    TBenchmarkMapperSkiff() = default;
    TBenchmarkMapperSkiff(const TTableSchema& tableSchema) 
        : TableSchemaNode_(tableSchema.ToNode()) {}

    void Do(const TRawJobContext& context) override {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TBufferedInput input(&unbufferedInput);
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList() [0]);
        TBufferedOutput output(&unbufferedOutput);
        
        TSkiffRowReader reader(&input, {TTableSchema::FromNode(TableSchemaNode_)});
        TSkiffRowWriter writer({&output}, {TTableSchema::FromNode(TableSchemaNode_)});

        for (; reader.IsValid(); reader.Next()) {
            auto row = reader.ReadRow();
            writer.AddRow(std::move(row), 0);
        }

        writer.Finish(0);
    }

private:
    TNode TableSchemaNode_;
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
            
        auto reader = TArrowRowReader(&input);
        auto writer = TArrowRowWriter({&output},
            {TransformDatetimeColumns(RemoveReadingContextColumns(reader.ArrowSchema()))});

        for (; reader.IsValid(); reader.Next()) {
                auto row = reader.ReadRow();
                writer.AddRow(std::move(row), 0);
        }
        
        writer.Finish(0);
    }
};
REGISTER_RAW_JOB(TBenchmarkMapperArrow);

}

int main(int argc, char** argv) {
    Initialize(argc, argv);

    Y_ENSURE(argc > 1, "Missing `format` argument!");
    TString format = argv[1];

    auto client = CreateClient("127.0.0.1:8000");
    TString inputTable = "//home/complex_types";
    TString outputTable = "//tmp/out_" + format;

    [[maybe_unused]] TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));

    // auto outputSchemaNode = TNode()
    //                             .Add(TNode()("name", "id")("type", "uint64"))
    //                             .Add(TNode()("name", "name")("type", "string"))
    //                             .Add(TNode()("name", "coef")("type", "double"));
    // auto outputTableSchema = TTableSchema::FromNode(outputSchemaNode);
    
    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", inputTableSchema.ToNode())));

    if (format == "yson") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new ComplexTypes::TBenchmarkMapperYson);
    } else if (format == "skiff") {
        auto skiffTableSchema = SkiffSchemaFromTableSchema(inputTableSchema);
    
        auto skiffOptions = NYT::NDetail::TCreateSkiffSchemaOptions();
        auto inputTableSkiffSchema = NYT::NDetail::CreateSkiffSchema({skiffTableSchema}, skiffOptions);
        auto outputTableSkiffSchema = NSkiff::CreateVariant16Schema({skiffTableSchema});
    
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(inputTableSkiffSchema)))
                .OutputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(outputTableSkiffSchema))),
            new ComplexTypes::TBenchmarkMapperSkiff(inputTableSchema));
    } else if (format == "arrow") {
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat("arrow"))
                .OutputFormat(TFormat("arrow")),
            new ComplexTypes::TBenchmarkMapperArrow);
    } else {
        ythrow yexception() << "Unknown format \"" + format + "\". It should be yson/skiff/protobuf.";
    }

    return 0;
}