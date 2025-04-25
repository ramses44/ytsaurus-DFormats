#include <util/stream/output.h>
#include <util/system/user.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>

#include <iostream>
#include <map>

#include <dformats/skiff_reader.h>
#include <dformats/skiff_writer.h>

using namespace NYT;

class TMultiplyMapperSkiff : public IRawJob {
public:
    Y_SAVELOAD_JOB(TableSchemaNode_, MultiplyCoef);

    TMultiplyMapperSkiff() = default;
    TMultiplyMapperSkiff(const TTableSchema& tableSchema, size_t multiplyCoef = 1) 
        : TableSchemaNode_(tableSchema.ToNode()), MultiplyCoef(multiplyCoef) {}

    void Do(const TRawJobContext& context) override {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TBufferedInput input(&unbufferedInput);
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList() [0]);
        TBufferedOutput output(&unbufferedOutput);
        
        TSkiffRowReader reader(&input, {TTableSchema::FromNode(TableSchemaNode_)});
        TSkiffRowWriter writer({&output}, {TTableSchema::FromNode(TableSchemaNode_)});

        for (; reader.IsValid(); reader.Next()) {
            auto row = reader.ReadRow();
            for (size_t i = 0; i < MultiplyCoef; ++i)
                writer.AddBuiltRow(row, 0);
        }

        writer.Finish(0);
    }

private:
    TNode TableSchemaNode_;
    size_t MultiplyCoef;
};
REGISTER_RAW_JOB(TMultiplyMapperSkiff);

class TMultiplyMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(MultiplyCoef);

    TMultiplyMapper(size_t multiplyCoef = 1) : MultiplyCoef(multiplyCoef) { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            
            for (size_t i = 0; i < MultiplyCoef; ++i)
                writer->AddRow(row);
        }
    }

private:
    size_t MultiplyCoef;
};
REGISTER_MAPPER(TMultiplyMapper);

int main(int argc, char** argv) {
    Initialize(argc, argv);

    auto client = CreateClient("127.0.0.1:8000");

    TString inputTable = argc > 1 ? argv[1] : "//home/all_types_table";
    TString outputTable = argc > 2 ? argv[2] : inputTable + "_big";
    size_t multiplyFactor = argc > 3 ? FromString<size_t>(argv[3]) : 100;

    TNode tableSchemaNode = client->Get(inputTable + "/@schema");
    TTableSchema tableSchema;
    Deserialize(tableSchema, tableSchemaNode);

    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", tableSchemaNode)));

    auto skiffTableSchema = SkiffSchemaFromTableSchema(tableSchema);
    auto skiffOptions = NYT::NDetail::TCreateSkiffSchemaOptions();
    auto inputTableSkiffSchema = NYT::NDetail::CreateSkiffSchema({skiffTableSchema}, skiffOptions);
    auto outputTableSkiffSchema = NSkiff::CreateVariant16Schema({skiffTableSchema});
    
    client->RawMap(
        TRawMapOperationSpec()
            .AddInput(inputTable)
            .AddOutput(outputTable)
            .InputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(inputTableSkiffSchema)))
            .OutputFormat(TFormat(NYT::NDetail::CreateSkiffFormat(outputTableSkiffSchema))),
        new TMultiplyMapperSkiff(tableSchema, multiplyFactor));

    // client->Map(
    //     TMapOperationSpec()
    //         .AddInput<TNode>(inputTable)
    //         .AddOutput<TNode>(outputTable),
    //     new TMultiplyMapper(multiplyFactor));

    return 0;
}