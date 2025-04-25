#include <util/stream/output.h>
#include <util/system/user.h>

#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/skiff_row_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

#include <iostream>
#include <map>

#include <dformats/arrow_reader.h>
#include <dformats/arrow_writer.h>

using namespace NYT;

void TestArrowWriting(IInputStream* input, IOutputStream* output) {
    auto reader = TArrowRowReader(input);

    auto newSchema = RemoveReadingContextColumns(reader.ArrowSchema());
    newSchema = TransformDatetimeColumns(newSchema);

    std::cerr << "New schema: " << newSchema->ToString() << std::endl;

    auto writer = TArrowRowWriter({output}, {newSchema}, {5});

    //PrintTypes(reader.ArrowSchema()); return;

    for (; reader.IsValid(); reader.Next()) {
            auto row = reader.ReadRow();
            writer.AddRow(std::move(row), 0);
    }
    
    writer.Finish(0);
}

class TestArrowJob : public IRawJob {
    public:
        void Do(const TRawJobContext& context) override {
            TUnbufferedFileInput unbufferedInput(context.GetInputFile());
            TBufferedInput input(&unbufferedInput);
            TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[0]);
            [[ maybe_unused ]] TBufferedOutput output(&unbufferedOutput);

            // std::cerr << input.ReadAll();
            // return;
    
            // THolder<IProxyOutput> rawWriter = ::MakeHolder<TJobWriter>(context.GetOutputFileList()); 
            // [[ maybe_unused ]] TNodeTableWriter writer(std::move(rawWriter));

            TestArrowWriting(&input, &output);
            return;

            auto reader = TArrowRowReader(&input);
            auto writer = TArrowRowWriter({&output}, {reader.ArrowSchema()}, {10});

            // PrintTypes(reader.ArrowSchema()); return;

            for (; reader.IsValid(); reader.Next()) {
                auto row = reader.ReadRow();

                // auto output = TNode();
                // output.Add(row->GetData<int64_t>(0));
                // output.Add(row->GetData<int32_t>(1));
                // output.Add(row->GetData<int16_t>(2));
                // output.Add(row->GetData<int8_t>(3));
                // output.Add(row->GetData<uint64_t>(4));
                // output.Add(row->GetData<uint32_t>(5));
                // output.Add(row->GetData<uint16_t>(6));
                // output.Add(row->GetData<uint8_t>(7));
                // output.Add(row->GetData<double>(8));
                // output.Add(row->GetData<float>(9));
                // output.Add(row->GetData<bool>(10));
                // output.Add(TString(row->GetData<std::string>(11)));
                // output.Add(TString(row->GetData<std::string>(12)));
                // output.Add(row->GetData<int32_t>(13));
                // output.Add(row->GetData<int64_t>(14));
                // output.Add(row->GetData<int64_t>(15));
                // output.Add(row->GetData<int64_t>(16));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(17)));
                // output.Add(TString(row->GetData<std::string>(18)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(19)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(20)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(21)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(22)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(23)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(24)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(25)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(26)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(27)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(28)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(29)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(30)));
                // output.Add(NodeFromYsonString(row->GetData<std::string>(31)));

                // writer.AddRow(TNode()("data", std::move(output)), 0);

                writer.AddRow(std::move(row), 0);
            }
    
            // writer.FinishTable(0);
            writer.Finish(0);
        }
};
REGISTER_RAW_JOB(TestArrowJob);

int main() {
    Initialize();

    // TFile inputFile("/home/ramses44/fqw/ytsaurus/dformats/examples/test/very_simple_table_data", OpenExisting | RdOnly);
    // TFile outputFile("/home/ramses44/fqw/ytsaurus/dformats/examples/test/out", WrOnly);
    // TFileInput inputStream(inputFile);
    // TFileOutput outputStream(outputFile);

    // TestArrowWriting(&inputStream, &outputStream);

    auto client = CreateClient("127.0.0.1:8000");
    // TString inputTable = "//tmp/very_simple_table";
    TString inputTable = "//home/all_types_table_big";
    TString outputTable = "//tmp/out";

    TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));

    // inputTable += "[:#100]";

    // [[ maybe_unused ]] auto outputSchemaNode = TNode().Add(TNode()("name", "data")("type", "any"));
    // [[ maybe_unused ]] auto outputTableSchema = TTableSchema::FromNode(outputSchemaNode);
    auto outputTableSchema = inputTableSchema;
    auto outputSchemaNode = outputTableSchema.ToNode();

    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", outputSchemaNode)));

    client->RawMap(
        TRawMapOperationSpec()
            .AddInput(inputTable)
            .AddOutput(outputTable)
            .InputFormat(TFormat(TNode("arrow")))
            .OutputFormat(TFormat(TNode("arrow"))),
        new TestArrowJob);

    return 0;
}