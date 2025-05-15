#include <dformats/interface/mapreduce.h>

#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>

#include <dformats/skiff/skiff_reader.h>
#include <dformats/skiff/skiff_writer.h>
#include <dformats/protobuf/protobuf_reader.h>
#include <dformats/protobuf/protobuf_writer.h>
#include <dformats/yson/yson_reader.h>
#include <dformats/yson/yson_writer.h>
#include <dformats/arrow/arrow_reader.h>
#include <dformats/arrow/arrow_writer.h>

namespace DFormats {

enum class Io : bool { Input, Output };

NYT::TFormat MakeFormat(enum Format format, const std::vector<NYT::TTableSchema>& tableSchemas,
    Io purpose, ReadingOptions readingOptions = static_cast<ReadingOptions>(0)) {

    switch (format) {
    case Format::Skiff: {
        TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas;
        skiffSchemas.reserve(tableSchemas.size());

        for (const auto& tableSchema : tableSchemas) {
            skiffSchemas.push_back(SkiffSchemaFromTableSchema(tableSchema));
        }

        if (purpose == Io::Input) {
            return NYT::NDetail::CreateSkiffFormat(NYT::NDetail::CreateSkiffSchema(skiffSchemas,
                NYT::NDetail::TCreateSkiffSchemaOptions()
                    .HasKeySwitch(readingOptions & ReadingOptions::HasAfterKeySwitch)
                    .HasRangeIndex(readingOptions & ReadingOptions::HasRangeIndex)));
        }
    
        return NYT::NDetail::CreateSkiffFormat(NSkiff::CreateVariant16Schema(skiffSchemas));
    }

    case Format::Protobuf: {
        THashMap<TString, NYT::TTableSchema> messageSchemas;

        for (size_t i = 0; i < tableSchemas.size(); ++i) {
            auto messageName = "DynamicMessage" + std::to_string(i);
            messageSchemas[messageName] = tableSchemas[i];
        }

        std::unique_ptr<google::protobuf::DescriptorPool> descriptorPool(MakeDescriptorPool(messageSchemas));
        TVector<const google::protobuf::Descriptor*> descriptors(tableSchemas.size());

        for (size_t i = 0; i < tableSchemas.size(); ++i) {
            auto messageName = "DynamicMessage" + std::to_string(i);
            descriptors[i] = descriptorPool->FindMessageTypeByName(messageName);
        }

        return TFormat::Protobuf(descriptors, true);
    }

    case Format::Yson:
        return NYT::TFormat("yson");

    case Format::Arrow:
        return NYT::TFormat("arrow");
    }
}

std::pair<NYT::TFormat, NYT::TFormat> MakeIOFormats(
    const MapReduceIOSchema& schema, ReadingOptions readingOptions) {

    std::vector<NYT::TTableSchema> inputSchemas;
    inputSchemas.reserve(schema.InputSchemaIndexes.size());
    for (size_t i : schema.InputSchemaIndexes) {
        inputSchemas.push_back(schema.TableSchemas[i]);
    }

    std::vector<NYT::TTableSchema> outputSchemas;
    outputSchemas.reserve(schema.OutputSchemaIndexes.size());
    for (size_t i : schema.OutputSchemaIndexes) {
        outputSchemas.push_back(schema.TableSchemas[i]);
    }

    return {MakeFormat(schema.InputFormat, inputSchemas, Io::Input, readingOptions),
            MakeFormat(schema.OutputFormat, outputSchemas, Io::Output)};
}

TNode MapReduceIOSchemaToNode(const MapReduceIOSchema& ioSchema) {
    auto res = TNode()
        ("TableSchemas", TNode::CreateList())
        ("InputFormat", ioSchema.InputFormat)
        ("InputSchemaIndexes", TNode::CreateList())
        ("OutputFormat", ioSchema.OutputFormat)
        ("OutputSchemaIndexes", TNode::CreateList());

    for (const auto& tableSchema : ioSchema.TableSchemas) {
        res["TableSchemas"].Add(tableSchema.ToNode());
    }
    for (auto ind : ioSchema.InputSchemaIndexes) {
        res["InputSchemaIndexes"].Add(ind);
    }
    for (auto ind : ioSchema.OutputSchemaIndexes) {
        res["OutputSchemaIndexes"].Add(ind);
    }

    return std::move(res);
}

MapReduceIOSchema MapReduceIOSchemaFromNode(const TNode& node) {
    MapReduceIOSchema res;

    for (const auto& tableSchema : node["TableSchemas"].AsList()) {
        res.TableSchemas.push_back(NYT::TTableSchema::FromNode(tableSchema));
    }

    res.InputFormat = static_cast<enum Format>(node["InputFormat"].AsInt64());
    res.OutputFormat = static_cast<enum Format>(node["OutputFormat"].AsInt64());

    for (const auto& ind : node["InputSchemaIndexes"].AsList()) {
        res.InputSchemaIndexes.push_back(ind.AsUint64());
    }
    for (const auto& ind : node["OutputSchemaIndexes"].AsList()) {
        res.OutputSchemaIndexes.push_back(ind.AsUint64());
    }

    return std::move(res);
}

TJob::TJob(MapReduceIOSchema ioSchema) : IOSchema_(std::move(ioSchema)) { }

void TJob::Do(const TRawJobContext& context) {
    std::unique_ptr<IRowReader> reader;
    std::unique_ptr<IRowWriter> writer;

    auto rawReader = MakeIntrusive<NYT::TJobReader>(context.GetInputFile());
    auto rawWriter = MakeHolder<NYT::TJobWriter>(context.GetOutputFileList());

    std::vector<NYT::TTableSchema> inputSchemas;
    inputSchemas.reserve(IOSchema_.InputSchemaIndexes.size());
    for (size_t i : IOSchema_.InputSchemaIndexes) {
        inputSchemas.push_back(IOSchema_.TableSchemas[i]);
    }
    std::vector<NYT::TTableSchema> outputSchemas;
    outputSchemas.reserve(IOSchema_.OutputSchemaIndexes.size());
    for (size_t i : IOSchema_.OutputSchemaIndexes) {
        outputSchemas.push_back(IOSchema_.TableSchemas[i]);
    }

    std::shared_ptr<TProtobufRowFactory> factory = 
        IOSchema_.InputFormat == Format::Protobuf || IOSchema_.OutputFormat == Format::Protobuf
        ? std::make_shared<TProtobufRowFactory>(IOSchema_.TableSchemas) : nullptr;

    switch (IOSchema_.InputFormat) {
    case Format::Skiff:
        reader.reset(new TSkiffRowReader(std::move(rawReader), std::move(inputSchemas)));
        break;
    case Format::Protobuf:
        reader.reset(new TProtobufRowReader(std::move(rawReader), factory, IOSchema_.InputSchemaIndexes));
        break;
    case Format::Yson:
        reader.reset(new TYsonRowReader(std::move(rawReader), std::move(inputSchemas)));
        break;
    case Format::Arrow:
        reader.reset(new TArrowRowReader(std::move(rawReader), std::move(inputSchemas)));
        break;
    }

    switch (IOSchema_.OutputFormat) {
    case Format::Skiff:
        writer.reset(new TSkiffRowWriter(std::move(rawWriter), std::move(outputSchemas)));
        break;
    case Format::Protobuf:
        writer.reset(new TProtobufRowWriter(std::move(rawWriter), factory, IOSchema_.OutputSchemaIndexes));
        break;
    case Format::Yson:
        writer.reset(new TYsonRowWriter(std::move(rawWriter), std::move(outputSchemas)));
        break;
    case Format::Arrow:
        writer.reset(new TArrowRowWriter(std::move(rawWriter), std::move(outputSchemas)));
        break;
    }

    DoImpl(reader.get(), writer.get());
}

void TJob::Save(IOutputStream& stream) const {
    MapReduceIOSchemaToNode(IOSchema_).Save(&stream);
}

void TJob::Load(IInputStream& stream) {
    TNode node;
    node.Load(&stream);
    IOSchema_ = MapReduceIOSchemaFromNode(node);
}

const MapReduceIOSchema& TJob::GetIOSchema() const {
    return IOSchema_;
}

}