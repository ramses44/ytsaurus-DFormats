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

namespace SimpleTen {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            auto row = cursor.GetRow();

            row["column_1"] = TNode(row["column_1"].AsInt64() + 1);
            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperProto
    : public IMapper<TTableReader<TSimpleTenMessage>, TTableWriter<TSimpleTenMessage>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            auto row = cursor.GetRow();

            row.set_column_1(row.column_1() + 1);
            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperProto);

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

            auto column_1 = *reinterpret_cast<const int64_t*>(row->GetRaw(0).data()) + 1;
            row->SetRaw(0, {reinterpret_cast<const char*>(&column_1), sizeof(column_1)});
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
                row->SetData(0, row->GetData<int64_t>(0) + 1);
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
    TString inputTable = "//home/simple_ten";
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
            new SimpleTen::TBenchmarkMapperYson);
    } else if (format == "protobuf") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TSimpleTenMessage>(inputTable)
                .AddOutput<TSimpleTenMessage>(outputTable),
            new SimpleTen::TBenchmarkMapperProto);
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
            new SimpleTen::TBenchmarkMapperSkiff(inputTableSchema));
    } else if (format == "arrow") {
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat("arrow"))
                .OutputFormat(TFormat("arrow")),
            new SimpleTen::TBenchmarkMapperArrow);
    } else {
        ythrow yexception() << "Unknown format \"" + format + "\". It should be yson/skiff/protobuf.";
    }

    return 0;
}