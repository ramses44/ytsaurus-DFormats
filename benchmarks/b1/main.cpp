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
#include <boost/preprocessor/iteration/local.hpp>

#include <dformats/skiff_reader.h>
#include <dformats/skiff_writer.h>
#include <dformats/arrow_reader.h>
#include <dformats/arrow_writer.h>
#include <dformats/benchmarks/bench.pb.h>

using namespace NYT;

namespace ThousandNumeric {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        size_t notNullCount = 0;
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            for (const auto& [key, value] : row.AsMap()) {
                if (!value.IsNull()) {
                    ++notNullCount;
                }
            }
        }

        writer->AddRow(TNode()("data", TNode()("not_null_count", notNullCount)));
        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperProto
    : public IMapper<TTableReader<TThousandNumericMessage>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        size_t notNullCount = 0;
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            #define BOOST_PP_LOCAL_MACRO(n)\
            if (row.column_##n()) ++notNullCount;
            #define BOOST_PP_LOCAL_LIMITS (1,1000)
            #include BOOST_PP_LOCAL_ITERATE()
        }

        writer->AddRow(TNode()("data", TNode()("not_null_count", notNullCount)));
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
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList() [0]);
        TBufferedOutput output(&unbufferedOutput);
        
        TSkiffRowReader reader(&input, {TTableSchema::FromNode(InputTableSchemaNode_)});
        //TSkiffRowWriter writer({&output}, {TTableSchema::FromNode(OutputTableSchemaNode_)});
        TNodeTableWriter writer(THolder<IProxyOutput>(MakeHolder<TJobWriter>(context.GetOutputFileList())));

        size_t notNullCount = 0;
        for (size_t i = 0; reader.IsValid(); reader.Next(), ++i) {
            auto row = reader.ReadRow();

            for (int i = 0; i < 1000; ++i) {
                auto rawData = row->GetRaw(i);
                if (!rawData.empty()) {
                    ++notNullCount;
                }
            }
        }

        writer.AddRow(TNode()("data", TNode()("not_null_count", notNullCount)), 0);
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

        size_t fieldsCount = RemoveReadingContextColumns(reader.ArrowSchema())->num_fields();
        size_t notNullCount = 0;
        for (; reader.IsValid(); reader.Next()) {
            auto row = reader.ReadRow();

            for (size_t i = 0; i < fieldsCount; ++i) {
                if (!row->IsNull(i)) ++notNullCount;
            }
        }
        
        writer.AddRow(TNode()("data", TNode()("not_null_count", notNullCount)), 0);
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
    TString inputTable = "//home/thousand_numeric";
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
            new ThousandNumeric::TBenchmarkMapperYson);
    } else if (format == "protobuf") {
        client->Map(
            TMapOperationSpec()
                .AddInput<TThousandNumericMessage>(inputTable)
                .AddOutput<TNode>(outputTable),
            new ThousandNumeric::TBenchmarkMapperProto);
    } else if (format == "skiff") {
        auto skiffTableSchema = SkiffSchemaFromTableSchema(inputTableSchema);
    
        auto skiffOptions = NYT::NDetail::TCreateSkiffSchemaOptions();
        auto inputTableSkiffSchema = NYT::NDetail::CreateSkiffSchema({skiffTableSchema}, skiffOptions);
        // auto outputTableSkiffSchema = NSkiff::CreateVariant16Schema({skiffTableSchema});
    
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(inputTableSkiffSchema)))
                .OutputFormat(TFormat("yson")),
            new ThousandNumeric::TBenchmarkMapperSkiff(inputTableSchema));
    } else if (format == "arrow") {
        client->RawMap(
            TRawMapOperationSpec()
                .AddInput(inputTable)
                .AddOutput(outputTable)
                .InputFormat(TFormat("arrow"))
                .OutputFormat(TFormat("yson")),
            new ThousandNumeric::TBenchmarkMapperArrow);
    } else {
        ythrow yexception() << "Unknown format \"" + format + "\". It should be yson/skiff/protobuf.";
    }

    return 0;
}