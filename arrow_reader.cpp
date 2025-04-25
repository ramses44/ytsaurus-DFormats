#include "arrow_reader.h"
#include "arrow_adapter.h"

void PrintTypes(TArrowSchemaPtr schema) {
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        std::cerr << "Column #" << i << " " << schema->field_names()[i] << " with type " << field->type()->ToString() << "\n";
    }
}

bool IsReadingContextColumnName(std::string_view name) {
    return name == "$table_index" || name == "$range_index" || 
           name == "$row_index" || name == "$key_switch";
}

TArrowSchemaPtr RemoveReadingContextColumns(TArrowSchemaPtr schema) {
    ::FieldVector fields;
    fields.reserve(schema->num_fields());
    
    for (const auto& field : schema->fields()) {
        if (!IsReadingContextColumnName(field->name())) {
            fields.push_back(field);
        }
    }

    return std::make_shared<::Schema>(std::move(fields));
}

std::any GetDataFromArray(std::shared_ptr<Array> array, int index) {
    Y_ENSURE(0 <= index && index < array->length(),
             "Invalid index. It must be in range [0; " + std::to_string(array->length()) + ").");

    if (array->IsNull(index)) {
        return std::any();
    }

    switch (array->type_id()) {
        case Type::BOOL:
            return std::any(std::static_pointer_cast<BooleanArray>(array)->Value(index));
        case Type::INT8:
            return std::any(std::static_pointer_cast<NumericArray<Int8Type>>(array)->Value(index));
        case Type::INT16:
            return std::any(std::static_pointer_cast<NumericArray<Int16Type>>(array)->Value(index));
        case Type::INT32:
        case Type::DATE32:
            return std::any(std::static_pointer_cast<NumericArray<Int32Type>>(array)->Value(index));
        case Type::INT64:
        case Type::DATE64:
        case Type::TIMESTAMP:
            return std::any(std::static_pointer_cast<NumericArray<Int64Type>>(array)->Value(index));
        case Type::UINT8:
            return std::any(std::static_pointer_cast<NumericArray<UInt8Type>>(array)->Value(index));
        case Type::UINT16:
            return std::any(std::static_pointer_cast<NumericArray<UInt16Type>>(array)->Value(index));
        case Type::UINT32:
            return std::any(std::static_pointer_cast<NumericArray<UInt32Type>>(array)->Value(index));
        case Type::UINT64:
            return std::any(std::static_pointer_cast<NumericArray<UInt64Type>>(array)->Value(index));
        case Type::FLOAT:
            return std::any(std::static_pointer_cast<NumericArray<FloatType>>(array)->Value(index));
        case Type::DOUBLE:
            return std::any(std::static_pointer_cast<NumericArray<DoubleType>>(array)->Value(index));
        case Type::STRING:
            return std::any(std::static_pointer_cast<StringArray>(array)->GetString(index));
        case Type::BINARY:
            return std::any(std::static_pointer_cast<BinaryArray>(array)->GetString(index));
        case Type::DICTIONARY:
        {
            auto dictArray = std::static_pointer_cast<DictionaryArray>(array);
            if (dictArray->indices()->IsNull(index)) {
                return std::any();
            }
            auto arrIndex = dictArray->GetValueIndex(index);
            return GetDataFromArray(dictArray->dictionary(), arrIndex);
        }
        default:  // TODO: Поддержать List, Map и Struct (на будущее)
            ythrow yexception() << "Trying to read unsopported ARROW type " << array->type()->ToString();
        }
}

TArrowRowReader::TArrowRowReader(std::shared_ptr<io::InputStream> underlying) {
    auto result = ipc::RecordBatchStreamReader::Open(underlying);
    Y_ENSURE(result.ok(), "Error occured while openning Arrow stream reader: " << result.status().ToString());
    Underlying_ = *result;

    Next();
}

TArrowRowReader::TArrowRowReader(IInputStream* input)
  : TArrowRowReader(std::make_shared<TArrowInputStreamAdapter>(input)) { }

TArrowSchemaPtr TArrowRowReader::ArrowSchema() const {
    return Underlying_->schema();
}

std::unique_ptr<TArrowRow> TArrowRowReader::ReadRow() {
    SkipRow();

    auto schema = ArrowSchema();
    auto row = std::make_unique<TArrowRow>(TransformDatetimeColumns(RemoveReadingContextColumns(schema)));

    for (int i = 0, excluded = 0; i < schema->num_fields(); ++i) {
        if (IsReadingContextColumnName(schema->field(i)->name())) {
            ++excluded;
            continue;
        }
        auto column = CurrentBatch_->column(i);

        row->SetData(i - excluded, GetDataFromArray(std::move(column), CurrentBatchRowId_));
    }

    return std::move(row);
}

void TArrowRowReader::SkipRow() {
    Y_ENSURE(IsValid(), "Trying to read row from empty batch");
}

bool TArrowRowReader::IsValid() const {
    return CurrentBatch_ && CurrentBatch_->num_rows() > CurrentBatchRowId_;
}

void TArrowRowReader::Next() {
    ++CurrentBatchRowId_;

    if (!IsValid()) {
        auto result = Underlying_->ReadNext(&CurrentBatch_);
        Y_ENSURE(result.ok(), "Reading ARROW-batch error: " + result.ToString());
        CurrentBatchRowId_ = 0;
        std::cerr << "Batch change\n";
    }
}

uint16_t TArrowRowReader::GetTableIndex() const {
    Y_ENSURE(IsValid(), "Trying to get reading context from invalid reader!");
    auto colId = ArrowSchema()->GetFieldIndex("$table_index");
    
    if (colId == -1) {
        return -1;
    }

    auto column = CurrentBatch_->column(colId);
    return std::static_pointer_cast<NumericArray<Int64Type>>(column)->Value(CurrentBatchRowId_);
}

int64_t TArrowRowReader::GetRangeIndex() const {
    Y_ENSURE(IsValid(), "Trying to get reading context from invalid reader!");
    auto colId = ArrowSchema()->GetFieldIndex("$range_index");
    
    if (colId == -1) {
        return -1;
    }

    auto column = CurrentBatch_->column(colId);
    return std::static_pointer_cast<NumericArray<Int64Type>>(column)->Value(CurrentBatchRowId_);
}

int64_t TArrowRowReader::GetRowIndex() const {
    Y_ENSURE(IsValid(), "Trying to get reading context from invalid reader!");
    auto colId = ArrowSchema()->GetFieldIndex("$row_index");
    
    if (colId == -1) {
        return -1;
    }

    auto column = CurrentBatch_->column(colId);
    return std::static_pointer_cast<NumericArray<Int64Type>>(column)->Value(CurrentBatchRowId_);
}

bool TArrowRowReader::IsAfterKeySwitch() const {
    Y_ENSURE(IsValid(), "Trying to get reading context from invalid reader!");
    auto colId = ArrowSchema()->GetFieldIndex("$key_switch");
    
    if (colId == -1) {
        return false;
    }

    auto column = CurrentBatch_->column(colId);
    return std::static_pointer_cast<BooleanArray>(column)->Value(CurrentBatchRowId_);
}

TReadingContext TArrowRowReader::GetReadingContext() const {
    return {GetTableIndex(), IsAfterKeySwitch(), GetRowIndex(), GetRangeIndex()};
}



