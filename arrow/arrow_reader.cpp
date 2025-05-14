#include "arrow_reader.h"
#include "arrow_adapter.h"

#include <library/cpp/yson/node/node_io.h> // TODO: remove

namespace DFormats {

TNode GetDataFromArray(std::shared_ptr<Array> array, int index) {
    Y_ENSURE(0 <= index && index < array->length(),
             "Invalid index. It must be in range [0; " + std::to_string(array->length()) + ").");

    if (array->IsNull(index)) {
        return TNode::CreateEntity();
    }

    switch (array->type_id()) {
    case Type::BOOL:
        return TNode(std::static_pointer_cast<BooleanArray>(array)->Value(index));
    case Type::INT8:
        return TNode(std::static_pointer_cast<NumericArray<Int8Type>>(array)->Value(index));
    case Type::INT16:
        return TNode(std::static_pointer_cast<NumericArray<Int16Type>>(array)->Value(index));
    case Type::INT32:
    case Type::DATE32:
        return TNode(std::static_pointer_cast<NumericArray<Int32Type>>(array)->Value(index));
    case Type::INT64:
    case Type::DATE64:
    case Type::TIMESTAMP:
        return TNode(std::static_pointer_cast<NumericArray<Int64Type>>(array)->Value(index));
    case Type::UINT8:
        return TNode(static_cast<uint64_t>(std::static_pointer_cast<NumericArray<UInt8Type>>(array)->Value(index)));
    case Type::UINT16:
        return TNode(static_cast<uint64_t>(std::static_pointer_cast<NumericArray<UInt16Type>>(array)->Value(index)));
    case Type::UINT32:
        return TNode(static_cast<uint64_t>(std::static_pointer_cast<NumericArray<UInt32Type>>(array)->Value(index)));
    case Type::UINT64:
        return TNode(std::static_pointer_cast<NumericArray<UInt64Type>>(array)->Value(index));
    case Type::FLOAT:
        return TNode(std::static_pointer_cast<NumericArray<FloatType>>(array)->Value(index));
    case Type::DOUBLE:
        return TNode(std::static_pointer_cast<NumericArray<DoubleType>>(array)->Value(index));
    case Type::STRING:
        return TNode(std::static_pointer_cast<StringArray>(array)->GetString(index));
    case Type::BINARY:
        return TNode(std::static_pointer_cast<BinaryArray>(array)->GetString(index));
    case Type::DICTIONARY: {
        auto dictArray = std::static_pointer_cast<DictionaryArray>(array);
        if (dictArray->indices()->IsNull(index)) {
            return TNode::CreateEntity();
        }
        auto arrIndex = dictArray->GetValueIndex(index);
        return GetDataFromArray(dictArray->dictionary(), arrIndex);
    }
    default:
        ythrow yexception() << "Trying to read unsopported ARROW type " << array->type()->ToString();
    }
}

TArrowRowReader::TArrowRowReader(::TIntrusivePtr<TRawTableReader> input, std::vector<NYT::TTableSchema> schemas)
  : Underlying_(std::move(input)), TableSchemas_(std::move(schemas)) {

    auto result = ipc::RecordBatchStreamReader::Open(std::make_shared<TArrowInputStreamAdapter>(Underlying_.Get()));
    Y_ENSURE(result.ok(), "Error occured while openning Arrow stream reader: " << result.status().ToString());
    ArrowStream_ = *result;

    Next();
}

IRowPtr TArrowRowReader::ReadRow() {
    Y_ENSURE(IsValid(), "Trying to read row from empty batch");

    auto schema = GetArrowSchema();
    auto row = std::make_shared<TArrowRow>(TableSchemas_[ReadingContext_.TableIndex]);
    auto fieldsNames = row->FieldsNames();

    for (int i = 0, excluded = 0; i < schema->num_fields(); ++i) {
        if (IsReadingContextColumnName(schema->field(i)->name())) {
            ++excluded;
            continue;
        }

        auto column = CurrentBatch_->column(i);
        row->Underlying()[fieldsNames[i - excluded]] = GetDataFromArray(std::move(column), CurrentBatchRowId_);
    }

    return std::move(row);
}

bool TArrowRowReader::IsValid() const {
    return CurrentBatch_ && CurrentBatch_->num_rows() > CurrentBatchRowId_;
}

bool TArrowRowReader::IsEndOfStream() const {
    return !CurrentBatch_;
}

void TArrowRowReader::Next() {
    ++CurrentBatchRowId_;

    if (!IsValid()) {
        auto result = ArrowStream_->ReadNext(&CurrentBatch_);
        Y_ENSURE(result.ok(), "Reading ARROW-batch error: " + result.ToString());
        CurrentBatchRowId_ = 0;
    }

    ReadingContext_ = {};

    if (IsEndOfStream()) {
        return;
    }

    auto schema = GetArrowSchema();
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& name = schema->field(i)->name();

        if (IsReadingContextColumnName(name)) {
            auto data = GetDataFromArray(CurrentBatch_->column(i), CurrentBatchRowId_);

            if (!data.HasValue()) {
                continue;
            }

            if (name == "$table_index") {
                ReadingContext_.TableIndex = data.AsInt64();
            } else if (name == "$row_index") {
                ReadingContext_.RowIndex = data.AsInt64();
            } else if (name == "$range_index") {
                ReadingContext_.RangeIndex = data.AsInt64();
            } else if (name == "$key_switch") {
                ReadingContext_.RangeIndex = data.AsBool();
            } 
        }
    }
}

const TReadingContext& TArrowRowReader::GetReadingContext() const {
    return ReadingContext_;
}

enum Format TArrowRowReader::Format() const {
    return Format::Arrow;
}

size_t TArrowRowReader::GetTablesCount() const {
    return TableSchemas_.size();
}

const NYT::TTableSchema& TArrowRowReader::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

TArrowSchemaPtr TArrowRowReader::GetArrowSchema() const {
    return ArrowStream_->schema();
}

}
