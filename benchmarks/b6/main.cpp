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

namespace SimpleTenWrOnly {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* /* reader */, TWriter* writer) override {
        for (size_t i = 0; i < 1000; ++i) {
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
            
            writer->AddRow(std::move(row));
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperProto
    : public IMapper<TTableReader<TSimpleTenMessage>, TTableWriter<TSimpleTenMessage>>
{
public:
    void Do(TReader* /* reader */, TWriter* writer) override {
        for (size_t i = 0; i < 1000; ++i) {
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
            
            writer->AddRow(std::move(row));
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
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[0]);
        TBufferedOutput output(&unbufferedOutput);
    
        TSkiffRowWriter writer({&output}, {TTableSchema::FromNode(TableSchemaNode_)});

        for (size_t i = 0; i < 1000; ++i) {
            auto row = TSkiffRow::MakeNew(writer.SkiffSchema(0));
            int64_t column_1 = -234234324;
            row->SetRaw(0, {reinterpret_cast<const char*>(&column_1), sizeof(column_1)});
            bool column_2 = true;
            row->SetRaw(1, {reinterpret_cast<const char*>(&column_2), sizeof(column_2)});
            int16_t column_3 = -42;
            row->SetRaw(2, {reinterpret_cast<const char*>(&column_3), sizeof(column_3)});
            double column_4 = 3.1415926;
            row->SetRaw(3, {reinterpret_cast<const char*>(&column_4), sizeof(column_4)});
            const char column_5[] = "\x0c\x00\x00\x00Hello world!";
            row->SetRaw(4, {column_5, 16});
            uint32_t column_6 = 45423456;
            row->SetRaw(5, {reinterpret_cast<const char*>(&column_6), sizeof(column_6)});
            uint64_t column_7 = 34567;
            row->SetRaw(6, {reinterpret_cast<const char*>(&column_7), sizeof(column_7)});
            const char column_8[] = "\x08\x00\x00\x00YTsaurus";
            row->SetRaw(7, {column_8, 12});
            bool column_9 = false;
            row->SetRaw(8, {reinterpret_cast<const char*>(&column_9), sizeof(column_9)});
            uint16_t column_10 = 42;
            row->SetRaw(9, {reinterpret_cast<const char*>(&column_10), sizeof(column_10)});
            
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

            for (size_t i = 0; i < 1000; ++i) {
                auto row = std::make_unique<TArrowRow>(writer.ArrowSchema(0));
                row->SetData<int64_t>(0, -234234324);
                row->SetData<bool>(1, true);
                row->SetData<int16_t>(2, -42);
                row->SetData<double>(3, 3.1415926);
                row->SetData<std::string>(4, "Hello world!");
                row->SetData<uint32_t>(5, 45423456);
                row->SetData<uint64_t>(6, 34567);
                row->SetData<std::string>(7, "YTsaurus");
                row->SetData<bool>(8, false);
                row->SetData<uint16_t>(9, 42);
                
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
            new SimpleTenWrOnly::TBenchmarkMapperYson);
    } else if (format == "protobuf") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TSimpleTenMessage>(inputTable)
                .AddOutput<TSimpleTenMessage>(outputTable),
            new SimpleTenWrOnly::TBenchmarkMapperProto);
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
            new SimpleTenWrOnly::TBenchmarkMapperSkiff(inputTableSchema));
    } else if (format == "arrow") {
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat("arrow"))
                .OutputFormat(TFormat("arrow")),
            new SimpleTenWrOnly::TBenchmarkMapperArrow);
    } else {
        ythrow yexception() << "Unknown format \"" + format + "\". It should be yson/skiff/protobuf.";
    }

    return 0;
}