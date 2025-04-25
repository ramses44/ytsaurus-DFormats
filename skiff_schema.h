#pragma once

#include <yt/cpp/mapreduce/client/skiff.h>

using namespace NYT;

NSkiff::TSkiffSchemaPtr SkiffSchemaFromTypeV3(NTi::TTypePtr type);
NSkiff::TSkiffSchemaPtr SkiffSchemaFromTableSchema(const TTableSchema& tableSchema);