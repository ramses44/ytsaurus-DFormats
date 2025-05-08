#pragma once

#include <yt/cpp/mapreduce/client/skiff.h>

using namespace NYT;

namespace DFormats {

NSkiff::TSkiffSchemaPtr SkiffSchemaFromTypeV3(NTi::TTypePtr type);
NSkiff::TSkiffSchemaPtr SkiffSchemaFromTableSchema(const TTableSchema& tableSchema);

int64_t SkiffSchemaStaticSize(const NSkiff::TSkiffSchemaPtr& schema);

}
