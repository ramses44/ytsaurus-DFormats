#pragma once

#include <vector>

#include <yt/cpp/mapreduce/interface/common.h>
#include <library/cpp/type_info/type_info.h>
#include <library/cpp/type_info/fwd.h>

namespace DFormats {

inline NTi::TTypePtr TableSchemaToStructType(const NYT::TTableSchema& schema) {
    std::vector<NTi::TStructType::TOwnedMember> fields;
    fields.reserve(schema.Columns().size());

    for (const auto& column : schema.Columns()) {
        fields.emplace_back(column.Name(), column.TypeV3());
    }

    return NTi::Struct(std::move(fields));
}

}
