#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/client/skiff.h>

#include "skiff_schema.h"
#include "skiff_types.h"

using namespace NYT;

class TSkiffRowWriter {
public:
    TSkiffRowWriter(TVector<IOutputStream*> outputs,
                    TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas)
      : Underlying_(std::move(outputs))
      , SkiffSchema_(NSkiff::CreateVariant16Schema(std::move(skiffSchemas))) { }

    TSkiffRowWriter(TVector<IOutputStream*> outputs,
                    const TVector<TTableSchema>& tableSchemas)
      : Underlying_(std::move(outputs)) {

        TVector<NSkiff::TSkiffSchemaPtr> skiffSchemas;
        skiffSchemas.reserve(skiffSchemas.size());
        for (const auto& tableSchema : tableSchemas) {
            skiffSchemas.emplace_back(SkiffSchemaFromTableSchema(tableSchema));
        }

        SkiffSchema_ = NSkiff::CreateVariant16Schema(std::move(skiffSchemas));
    }

    void Finish(uint16_t tableIndex) {
        Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");
        Underlying_[tableIndex]->Finish();
    }

    void AddRow(std::unique_ptr<TSkiffRow> row, size_t tableIndex) {
        Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");

        row->Rebuild();

        auto* stream = Underlying_[tableIndex];

        stream->Write(&tableIndex, 2);
        stream->Write(row->Data_.Data(), row->Data_.Size());
    }

    void AddBuiltRow(const std::unique_ptr<TSkiffRow>& row, size_t tableIndex) {
        Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");

        auto* stream = Underlying_[tableIndex];

        stream->Write(&tableIndex, 2);
        stream->Write(row->Data_.Data(), row->Data_.Size());
    }

    NSkiff::TSkiffSchemaPtr SkiffSchema(size_t tableIndex) const {
        return SkiffSchema_->GetChildren()[tableIndex];
    }

protected:
    TVector<IOutputStream*> Underlying_;
    NSkiff::TSkiffSchemaPtr SkiffSchema_;
};