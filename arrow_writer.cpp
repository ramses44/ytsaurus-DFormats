#include "arrow_writer.h"

std::shared_ptr<Scalar> MakeScalar(const TArrowRow& row, int fieldInd) {
    Y_ENSURE(fieldInd < row.GetArrowSchema()->num_fields(), "Invalid field index");

    if (row.IsNull(fieldInd)) {
        return nullptr;
    }

    std::shared_ptr<DataType> fieldType = row.GetArrowSchema()->field(fieldInd)->type();
    Result<std::shared_ptr<Scalar>> result;

    switch (fieldType->id()) {
    case Type::INT8:
        result = MakeScalar(row.GetData<int8_t>(fieldInd));
        break;
    case Type::INT16:
        result = MakeScalar(row.GetData<int16_t>(fieldInd));
        break;
    case Type::INT32:
        result = MakeScalar(row.GetData<int32_t>(fieldInd));
        break;
    case Type::INT64:
        result = MakeScalar(row.GetData<int64_t>(fieldInd));
        break;
    case Type::UINT8:
        result = MakeScalar(row.GetData<uint8_t>(fieldInd));
        break;
    case Type::UINT16:
        result = MakeScalar(row.GetData<uint16_t>(fieldInd));
        break;
    case Type::UINT32:
        result = MakeScalar(row.GetData<uint32_t>(fieldInd));
        break;
    case Type::UINT64:
        result = MakeScalar(row.GetData<uint64_t>(fieldInd));
        break;
    case Type::BOOL:
        result = MakeScalar(row.GetData<bool>(fieldInd));
        break;
    case Type::FLOAT:
        result = MakeScalar(row.GetData<float>(fieldInd));
        break;
    case Type::DOUBLE:
        result = MakeScalar(row.GetData<double>(fieldInd));
        break;
    case Type::BINARY:
    // case Type::BINARY:{
    //     auto scalar = std::make_shared<BinaryScalar>(
    //         std::make_shared<Buffer>(util::string_view(row.GetData<std::string>(fieldInd))));

    //     std::cerr << scalar->value->ToHexString() << std::endl;

    //     result = Result<std::shared_ptr<Scalar>>(scalar);
    //     break;
    // } 
    case Type::STRING:
        result = MakeScalar(row.GetData<std::string>(fieldInd));
        break;
    case Type::DATE32:
        result = MakeScalar(row.GetData<int32_t>(fieldInd));
        break;
    case Type::DATE64:
        result = MakeScalar(row.GetData<int64_t>(fieldInd));
        break;
    case Type::TIMESTAMP:
        result = MakeScalar<uint64_t>(row.GetData<int64_t>(fieldInd));
        break;
    default:
        ythrow yexception() << "Unsupported type: " << fieldType->ToString();
    }

    Y_ENSURE(result.ok(), "Error while making scalar from row data: " << result.status().ToString());

    return *result;
}

///// TArrowRowWriter

TArrowRowWriter::TArrowRowWriter(TVector<std::shared_ptr<io::OutputStream>> outputs,
  TVector<TArrowSchemaPtr> arrowSchemas, TVector<size_t> batchSizes)
  : ArrowSchemas_(std::move(arrowSchemas)) {

    BatchSizes_ = batchSizes ? std::move(batchSizes) : TVector<size_t>(outputs.size(), 1000);

    Y_ENSURE(outputs.size() == ArrowSchemas_.size() && ArrowSchemas_.size() == BatchSizes_.size(),
             "`outputs`, `arrowSchemas` and `batchSizes` vectors' sizes missmatch");

    Underlying_.reserve(outputs.size());

    for (size_t i = 0; i < outputs.size(); ++i) {
        auto result =  ipc::MakeStreamWriter(outputs[i], ArrowSchemas_[i]);
        Y_ENSURE(result.ok(), "Error occured while openning Arrow stream writer["
                              << i << "]: " << result.status().ToString());
                
        Underlying_.emplace_back(*result);
    }

    InitArrayBuilders();
}

TArrowRowWriter::TArrowRowWriter(TVector<IOutputStream*> outputs,
  TVector<TArrowSchemaPtr> arrowSchemas, TVector<size_t> batchSizes) {

    TVector<std::shared_ptr<io::OutputStream>> adaptedStreams;
    adaptedStreams.reserve(outputs.size());

    for (auto stream : outputs) {
        adaptedStreams.emplace_back(std::make_shared<TArrowOutputStreamAdapter>(stream));
    }

    *this = TArrowRowWriter(std::move(adaptedStreams),
                            std::move(arrowSchemas), std::move(batchSizes));
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
                // ArrayBuilders_.back().push_back(std::make_shared<TimestampBuilder>());
                ArrayBuilders_.back().push_back(std::make_shared<UInt64Builder>());
                break;
            default:
                ythrow yexception() << "Unsupported type: " << field->type()->ToString();
            }
        }
    }
}

void TArrowRowWriter::Finish(size_t tableIndex) {
    Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");

    WriteBatch(tableIndex);

    auto status = Underlying_[tableIndex]->Close();
    Y_ENSURE(status.ok(), "Error while closing stream #" << tableIndex << ": " << status.ToString());
}

void TArrowRowWriter::AddRow(std::unique_ptr<TArrowRow> row, size_t tableIndex) {
    Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");

    for (int colId = 0; colId < ArrowSchemas_[tableIndex]->num_fields(); ++colId) {
        auto status = row->IsNull(colId)
                    ? ArrayBuilders_[tableIndex][colId]->AppendNull() 
                    : ArrayBuilders_[tableIndex][colId]->AppendScalar(*MakeScalar(*row, colId));
        Y_ENSURE(status.ok(), "Builder append error: " << status.ToString());
    }

    if (ArrayBuilders_[tableIndex].front()->length() >= static_cast<int64_t>(BatchSizes_[tableIndex])) {
        WriteBatch(tableIndex);
    }
}

void TArrowRowWriter::WriteBatch(size_t tableIndex) {
    Y_ENSURE(tableIndex < Underlying_.size(), "Invalid table index");

    TVector<std::shared_ptr<Array>> arrays(ArrowSchemas_[tableIndex]->num_fields());
    
    for (int colId = 0; colId < ArrowSchemas_[tableIndex]->num_fields(); ++colId) {
        auto status = ArrayBuilders_[tableIndex][colId]->Finish(&arrays[colId]);
        Y_ENSURE(status.ok(), "Error while building data to array[" 
                              << tableIndex << "][" << colId << "]: " << status.ToString());
    }

    auto batch = RecordBatch::Make(ArrowSchemas_[tableIndex], arrays.front()->length(), arrays);

    auto status = Underlying_[tableIndex]->WriteRecordBatch(*batch);
    Y_ENSURE(status.ok(), "Error while writing batch to table #" 
                          << tableIndex << ": " << status.ToString());
}
