#pragma once

#include <vector>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/format.h>

#include "io.h"

namespace DFormats {

struct MapReduceIOSchema {
    std::vector<NYT::TTableSchema> TableSchemas;
    enum Format InputFormat;
    std::vector<size_t> InputSchemaIndexes;
    enum Format OutputFormat;
    std::vector<size_t> OutputSchemaIndexes;
};

class TJob : public NYT::IRawJob {
public:
    TJob() = default;
    TJob(MapReduceIOSchema ioSchema);

    void Do(const NYT::TRawJobContext& context) override;

    void Save(IOutputStream& stream) const override;
    void Load(IInputStream& stream) override;

    // Method to be implemented by users
    virtual void DoImpl(IRowReader* /* reader */, IRowWriter* /* writer */) {} 

protected:
    const MapReduceIOSchema& GetIOSchema() const;

private:
    MapReduceIOSchema IOSchema_;
};

std::pair<NYT::TFormat, NYT::TFormat> MakeIOFormats(const MapReduceIOSchema& schema,
    ReadingOptions readingOptions = static_cast<ReadingOptions>(0));

}