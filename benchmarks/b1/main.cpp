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

#include <chrono>

using namespace NYT;
using namespace DFormats;

namespace b1 {

class TBenchmarkMapperYson : public IMapper<TTableReader<TNode>, TTableWriter<TNode>> {
public:
    void Do(TReader* reader, TWriter* writer) override {
        [[maybe_unused]] size_t notNullCount = 0;
        double totalTime = 0;
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            auto start = std::chrono::high_resolution_clock::now();

            for (const auto& [key, value] : row.AsMap()) {
                if (!value.IsNull()) {
                    ++notNullCount;
                }
            }

            std::chrono::duration<double> duration = std::chrono::high_resolution_clock::now() - start;
            totalTime += duration.count();
        }

        writer->AddRow(TNode()("not_null_count", totalTime));
        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperYson);

class TBenchmarkMapperStaticProto : public IMapper<TTableReader<TThousandNumericMessage>, TTableWriter<TNode>> {
public:
    void Do(TReader* reader, TWriter* writer) override {
        [[maybe_unused]] size_t notNullCount = 0;
        double totalTime = 0;

        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            auto start = std::chrono::high_resolution_clock::now();

            #define BOOST_PP_LOCAL_MACRO(n)\
            if (row.column_##n()) ++notNullCount;
            #define BOOST_PP_LOCAL_LIMITS (1,1000)
            #include BOOST_PP_LOCAL_ITERATE()

            std::chrono::duration<double> duration = std::chrono::high_resolution_clock::now() - start;
            totalTime += duration.count();
        }

        writer->AddRow(TNode()("not_null_count", totalTime));
        writer->Finish();
    }
};
REGISTER_MAPPER(TBenchmarkMapperStaticProto);

class TBenchmarkMapper : public TJob {
public:
    template <typename... Args>
    TBenchmarkMapper(Args&&... args) : TJob(std::forward<Args>(args)...) { }

    void DoImpl(IRowReader* reader, IRowWriter* writer) {
        [[maybe_unused]] size_t notNullCount = 0;
        const auto& schema = GetIOSchema().TableSchemas[GetIOSchema().InputSchemaIndexes[0]];
        double totalTime = 0;

        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->ReadRow();

            auto start = std::chrono::high_resolution_clock::now();

            for (size_t i = 0; i < schema.Columns().size(); ++i) {
                switch(schema.Columns()[i].Type()) {
                case EValueType::VT_INT8:
                    notNullCount += static_cast<bool>(row->GetValue<int8_t>(i));
                    break;
                case EValueType::VT_INT16:
                    notNullCount += static_cast<bool>(row->GetValue<int16_t>(i));
                    break;
                case EValueType::VT_INT32:
                    notNullCount += static_cast<bool>(row->GetValue<int32_t>(i));
                    break;
                case EValueType::VT_INT64:
                    notNullCount += static_cast<bool>(row->GetValue<int64_t>(i));
                    break;
                case EValueType::VT_UINT8:
                    notNullCount += static_cast<bool>(row->GetValue<uint8_t>(i));
                    break;
                case EValueType::VT_UINT16:
                    notNullCount += static_cast<bool>(row->GetValue<uint16_t>(i));
                    break;
                case EValueType::VT_UINT32:
                    notNullCount += static_cast<bool>(row->GetValue<uint32_t>(i));
                    break;
                case EValueType::VT_UINT64:
                    notNullCount += static_cast<bool>(row->GetValue<uint64_t>(i));
                    break;
                case EValueType::VT_BOOLEAN:
                    notNullCount += row->GetValue<bool>(i);
                    break;
                case EValueType::VT_FLOAT:
                    notNullCount += static_cast<bool>(row->GetValue<float>(i));
                    break;
                case EValueType::VT_DOUBLE:
                    notNullCount += static_cast<bool>(row->GetValue<double>(i));
                    break;
                default:
                    break;
                }
            }

            std::chrono::duration<double> duration = std::chrono::high_resolution_clock::now() - start;
            totalTime += duration.count();
        }

        auto outputRow = writer->CreateObjectForWrite(0);
        outputRow->SetValue("not_null_count", totalTime);

        writer->WriteRow(std::move(outputRow), 0);
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
    TString inputTable = "//home/thousand_numeric";
    TString outputTable = "//tmp/out_" + format;

    [[maybe_unused]] TTableSchema inputTableSchema;
    Deserialize(inputTableSchema, client->Get(inputTable + "/@schema"));

    auto outputSchemaNode = TNode()
        .Add(TNode()("name", "not_null_count")("type", "double"));
    
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
            new b1::TBenchmarkMapperYson);
    } else if (format == "static-protobuf") {
        client->Map(
            TMapOperationSpec()
                .JobCount(jobCount)
                .AddInput<TThousandNumericMessage>(inputTable)
                .AddOutput<TNode>(outputTable),
            new b1::TBenchmarkMapperStaticProto);
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
            new b1::TBenchmarkMapper(std::move(ioSchema)));
    }

    return 0;
}