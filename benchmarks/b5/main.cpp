#include <util/stream/output.h>
#include <util/system/user.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <iostream>
#include <map>
#include <boost/preprocessor/iteration/local.hpp>

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

namespace b5 {

class TBenchmarkMapperYson
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            auto row = cursor.GetRow();

            for (TNode& elem : row["list"].AsList()) {
                elem = TNode(elem.AsDouble() + 1);
            }

            row["list"].Add(TNode(234234.23));

            writer->AddRow(row);
        }

        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperStaticProto
    : public IMapper<TTableReader<TComplexTypesMessage>, TTableWriter<TComplexTypesMessage>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            auto row = cursor.GetRow();

            for (int i = 0; i < row.list().list_size(); ++i) {
                row.mutable_list()->set_list(i, row.list().list(i) + 1);
            } 

            row.mutable_list()->add_list(42324.34);

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

    void DoImpl(IRowReader* reader, IRowWriter* writer) {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->ReadRow();
            auto list = row->GetValue<IListPtr>(1);

            size_t size = list->Size();
            for (size_t i = 0; i < size; ++i) {
                list->SetValue(i, list->GetValue<double>(i) + 1);
            }

            row->SetValue(1, std::move(list));

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
    TString inputTable = "//home/complex_types";
    TString outputTable = "//tmp/out_" + format;

    [[maybe_unused]] TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));
    
    client->Create(
        outputTable, ENodeType::NT_TABLE, TCreateOptions()
        .Force(true)
        .Attributes(
            TNode()
            ("schema", inputTableSchema.ToNode())));

    MapReduceIOSchema ioSchema = {
        {inputTableSchema},
        Format::Yson,  // For default
        {0},
        Format::Yson,  // For default
        {0}
    };

    if (format == "classic-yson") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new b5::TBenchmarkMapperYson);
    } else if (format == "static-protobuf") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TComplexTypesMessage>(inputTable)
                .AddOutput<TComplexTypesMessage>(outputTable),
            new b5::TBenchmarkMapperStaticProto);
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
            new b5::TBenchmarkMapper(std::move(ioSchema)));
    }

    return 0;
}