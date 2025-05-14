#include "arrow_writer.h"

using namespace DFormats;

std::shared_ptr<Scalar> MakeScalar(const TArrowRow& row, TArrowSchemaPtr arrowSchema, int fieldInd) {
    Y_ENSURE(fieldInd < arrowSchema->num_fields(), "Invalid field index");

    if (!row.Underlying()[arrowSchema->field(fieldInd)->name()].HasValue()) {
        return nullptr;
    }

    std::shared_ptr<DataType> fieldType = arrowSchema->field(fieldInd)->type();
    Result<std::shared_ptr<Scalar>> result;

    switch (fieldType->id()) {
    case Type::INT8:
        result = MakeScalar(row.GetValue<int8_t>(fieldInd));
        break;
    case Type::INT16:
        result = MakeScalar(row.GetValue<int16_t>(fieldInd));
        break;
    case Type::INT32:
        result = MakeScalar(row.GetValue<int32_t>(fieldInd));
        break;
    case Type::INT64:
        result = MakeScalar(row.GetValue<int64_t>(fieldInd));
        break;
    case Type::UINT8:
        result = MakeScalar(row.GetValue<uint8_t>(fieldInd));
        break;
    case Type::UINT16:
        result = MakeScalar(row.GetValue<uint16_t>(fieldInd));
        break;
    case Type::UINT32:
        result = MakeScalar(row.GetValue<uint32_t>(fieldInd));
        break;
    case Type::UINT64:
        result = MakeScalar(row.GetValue<uint64_t>(fieldInd));
        break;
    case Type::BOOL:
        result = MakeScalar(row.GetValue<bool>(fieldInd));
        break;
    case Type::FLOAT:
        result = MakeScalar(row.GetValue<float>(fieldInd));
        break;
    case Type::DOUBLE:
        result = MakeScalar(row.GetValue<double>(fieldInd));
        break;
    case Type::BINARY:
    // {
    //     auto scalar = std::make_shared<BinaryScalar>(
    //         std::make_shared<Buffer>(util::string_view(row.GetValue<std::string>(fieldInd))));
    //     result = Result<std::shared_ptr<Scalar>>(scalar);
    //     break;
    // } 
    case Type::STRING:
        result = MakeScalar(std::string(row.GetValue<std::string_view>(fieldInd)));
        break;
    case Type::DATE32:
        result = MakeScalar(row.GetValue<int32_t>(fieldInd));
        break;
    case Type::DATE64:
        result = MakeScalar(row.GetValue<int64_t>(fieldInd));
        break;
    case Type::TIMESTAMP:
        result = MakeScalar<uint64_t>(row.GetValue<int64_t>(fieldInd));
        break;
    default:
        ythrow yexception() << "Unsupported type: " << fieldType->ToString();
    }

    Y_ENSURE(result.ok(), "Error while making scalar from row data: " << result.status().ToString());

    return *result;
}

// TArrowRowWriter

TArrowRowWriter::TArrowRowWriter(THolder<IProxyOutput> output, std::vector<NYT::TTableSchema> schemas,
    std::vector<size_t> batchSizes) : Underlying_(std::move(output)), TableSchemas_(std::move(schemas)) {
    
    BatchSizes_ = !batchSizes.empty() ? std::move(batchSizes) : 
        std::vector<size_t>(TableSchemas_.size(), TArrowRowWriter::kDefaultBatchSize);
    
    Y_ENSURE(TableSchemas_.size() == Underlying_->GetStreamCount() &&
             TableSchemas_.size() == BatchSizes_.size());

    ArrowStreams_.reserve(TableSchemas_.size());

    for (size_t i = 0; i < TableSchemas_.size(); ++i) {
        ArrowSchemas_.push_back(MakeArrowSchema(TableSchemas_[i]));
        auto result =  ipc::MakeStreamWriter(
            std::make_shared<TArrowOutputStreamAdapter>(Underlying_->GetStream(i)), ArrowSchemas_[i]);
        Y_ENSURE(result.ok(), "Error occured while openning Arrow stream writer["
                              << i << "]: " << result.status().ToString());
                
        ArrowStreams_.emplace_back(*result);
    }

    InitArrayBuilders();
}

void TArrowRowWriter::InitArrayBuilders() {
    ArrayBuilders_.clear();

    for (const auto& schema : ArrowSchemas_) {
        ArrayBuilders_.emplace_back();

        for (const auto& field : schema->fields()) {
            switch (field->type()->id()) {
            case Type::INT8:
                ArrayBuilders_.back().push_back(std::make_shared<Int8Builder>());
                break;
            case Type::INT16:
                ArrayBuilders_.back().push_back(std::make_shared<Int16Builder>());
                break;
            case Type::INT32:
                ArrayBuilders_.back().push_back(std::make_shared<Int32Builder>());
                break;
            case Type::INT64:
                ArrayBuilders_.back().push_back(std::make_shared<Int64Builder>());
                break;
            case Type::UINT8:
                ArrayBuilders_.back().push_back(std::make_shared<UInt8Builder>());
                break;
            case Type::UINT16:
                ArrayBuilders_.back().push_back(std::make_shared<UInt16Builder>());
                break;
            case Type::UINT32:
                ArrayBuilders_.back().push_back(std::make_shared<UInt32Builder>());
                break;
            case Type::UINT64:
                ArrayBuilders_.back().push_back(std::make_shared<UInt64Builder>());
                break;
            case Type::BOOL:
                ArrayBuilders_.back().push_back(std::make_shared<BooleanBuilder>());
                break;
            case Type::FLOAT:
                ArrayBuilders_.back().push_back(std::make_shared<FloatBuilder>());
                break;
            case Type::DOUBLE:
                ArrayBuilders_.back().push_back(std::make_shared<DoubleBuilder>());
                break;
            case Type::BINARY:
                // ArrayBuilders_.back().push_back(std::make_shared<BinaryBuilder>());
                // break;
            case Type::STRING:
                ArrayBuilders_.back().push_back(std::make_shared<StringBuilder>());
                break;
            case Type::DATE32:
                ArrayBuilders_.back().push_back(std::make_shared<Int32Builder>());
                break;
            case Type::DATE64:
                ArrayBuilders_.back().push_back(std::make_shared<Int64Builder>());
                break;
            case Type::TIMESTAMP:
                ArrayBuilders_.back().push_back(std::make_shared<UInt64Builder>());
                break;
            default:
                ythrow yexception() << "Unsupported type: " << field->type()->ToString();
            }
        }
    }
}

void TArrowRowWriter::WriteRow(const IRowConstPtr& row, size_t tableIndex) {
    WriteRow(row->CopyRow(), tableIndex);
}

void TArrowRowWriter::WriteRow(IRowPtr&& row, size_t tableIndex) {
    Y_ENSURE(tableIndex < ArrowStreams_.size(), "Invalid table index");

    auto arrowRow = std::dynamic_pointer_cast<TArrowRow>(row);
    arrowRow->SerializeComplexNodes();

    for (int colId = 0; colId < ArrowSchemas_[tableIndex]->num_fields(); ++colId) {
        auto scalar = MakeScalar(*arrowRow, ArrowSchemas_[tableIndex], colId);
        auto status = scalar 
            ? ArrayBuilders_[tableIndex][colId]->AppendScalar(*scalar)
            : ArrayBuilders_[tableIndex][colId]->AppendNull();
                    
        Y_ENSURE(status.ok(), "Builder append error: " << status.ToString());
    }

    if (ArrayBuilders_[tableIndex].front()->length() >= static_cast<int64_t>(BatchSizes_[tableIndex])) {
        WriteBatch(tableIndex);
    }
}

void TArrowRowWriter::FinishTable(size_t tableIndex) {
    Y_ENSURE(tableIndex < ArrowStreams_.size(), "Invalid table index");

    WriteBatch(tableIndex);

    auto status = ArrowStreams_[tableIndex]->Close();
    Y_ENSURE(status.ok(), "Error while closing stream #" << tableIndex << ": " << status.ToString());
}

enum Format TArrowRowWriter::Format() const {
    return Format::Arrow;
}

size_t TArrowRowWriter::GetTablesCount() const {
    return ArrowStreams_.size();
}

const NYT::TTableSchema& TArrowRowWriter::GetTableSchema(size_t tableIndex) const {
    return TableSchemas_[tableIndex];
}

IRowPtr TArrowRowWriter::CreateObjectForWrite(size_t tableIndex) const {
    return std::make_shared<TArrowRow>(GetTableSchema(tableIndex));
}

TArrowSchemaPtr TArrowRowWriter::GetArrowSchema(size_t tableIndex) const {
    return ArrowSchemas_[tableIndex];
}

void TArrowRowWriter::WriteBatch(size_t tableIndex) {
    Y_ENSURE(tableIndex < ArrowStreams_.size(), "Invalid table index");

    std::vector<std::shared_ptr<Array>> arrays(ArrowSchemas_[tableIndex]->num_fields());
    
    for (int colId = 0; colId < ArrowSchemas_[tableIndex]->num_fields(); ++colId) {
        auto status = ArrayBuilders_[tableIndex][colId]->Finish(&arrays[colId]);
        Y_ENSURE(status.ok(), "Error while building data to array[" 
                              << tableIndex << "][" << colId << "]: " << status.ToString());
    }

    auto batch = RecordBatch::Make(ArrowSchemas_[tableIndex], arrays.front()->length(), arrays);

    auto status = ArrowStreams_[tableIndex]->WriteRecordBatch(*batch);
    Y_ENSURE(status.ok(), "Error while writing batch to table #" 
                          << tableIndex << ": " << status.ToString());
}
