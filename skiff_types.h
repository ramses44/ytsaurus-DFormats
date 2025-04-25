#pragma once

#include <library/cpp/skiff/skiff_schema.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <iostream>

using TStringView = std::string_view;

class TSkiffRowReader;
class TSkiffRowWriter;

TString SkiffSerializeString(TStringView str);

TString SkiffDeserializeString(const char* skiffStr);

TString EwireTypeStr(NSkiff::EWireType type);

class TSkiffData {
public:
    TSkiffData();
    TSkiffData(NSkiff::TSkiffSchemaPtr schema);
    TSkiffData(NSkiff::TSkiffSchemaPtr schema, TStringView skiffStr);
    TSkiffData(NSkiff::TSkiffSchemaPtr schema, TBuffer buf);

    virtual ~TSkiffData() = default;

    TString Serialize() const;
    TString Release();
    virtual void Rebuild();

protected:
    virtual void ReplaceData(TStringView newData);

protected:
    NSkiff::TSkiffSchemaPtr Schema_;
    mutable TBuffer Data_;

    static void ApplyReplaceData(TSkiffData* object, TStringView newData);
    static TBuffer& GetData(TSkiffData* object);
};

class TSkiffVariant : public TSkiffData {
public:
    TSkiffVariant();
    TSkiffVariant(NSkiff::TSkiffSchemaPtr schema);

    template <class T>
    TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, uint16_t tag, T data);

    // TODO: Попробовать сделать общий конструктор template<Args...> + TSkiffData(std::forward(...))
    // TODO: Сделать макрос для общих конструкторов типа тех, что идут ниже ↓

    TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, TStringView skiffStr);
    TSkiffVariant(NSkiff::TSkiffSchemaPtr schema, TBuffer&& buf);
    TSkiffVariant(const TSkiffVariant& rhs);
    TSkiffVariant(TSkiffVariant&& rhs) noexcept;

    TSkiffVariant& operator=(const TSkiffVariant& rhs);
    TSkiffVariant& operator=(TSkiffVariant&& rhs) noexcept;

    uint16_t GetTag() const;
    bool IsTerminal() const;

    // TODO: Возможно, стоит написать отдельные специализации для ВСЕХ разрешённых типов
    // А в generic-шаблоне кидать ошибку типа "Bad data type provided. It should be one of ..."
    template <class T>  // Либо тут написать концепт для валидации типа
    void SetData(uint16_t tag, T data);

public:
    static uint8_t Terminal8Tag();
    static uint16_t Terminal16Tag();

protected:
    void SetTag(uint16_t tag);
    void ReserveBuffer(size_t size);
    size_t TagSize() const;
};

// Есть идея, что мы организуем shared-буффер для всех объектов, которые возвращают геттеры
// Пишем в доке чётко, что по-умолчанию Row и полученные из неё поля шарят память
// В геттерах пусть будет параметр inplace=true/false, которым можно заставить всё-таки перекопировать данные
// И ещё в методе Rebuild пускай будет параметр force=false/true, чтобы насильно сериализовать объект в новой памяти

class TSkiffTuple : public TSkiffData {
public:
    static std::unique_ptr<TSkiffTuple> MakeNew(NSkiff::TSkiffSchemaPtr schema);

public:
    TSkiffTuple();
    TSkiffTuple(NSkiff::TSkiffSchemaPtr schema);
    TSkiffTuple(const TSkiffTuple& rhs);
    TSkiffTuple(TSkiffTuple&& rhs) noexcept;

    ~TSkiffTuple() override;

    TSkiffTuple& operator=(const TSkiffTuple& rhs);
    TSkiffTuple& operator=(TSkiffTuple&& rhs) noexcept;

    TStringView GetRaw(size_t ind) const;
    void SetRaw(size_t ind, TStringView data);

    void Rebuild() override;

protected:
    size_t SerializedFieldDataSize(size_t ind) const;  // Менять аккуратно, см. Rebuild()
    NSkiff::TSkiffSchemaPtr GetFieldSchema(size_t ind) const;
    template<class T = char>
    T* GetFieldDataPtr(size_t ind);
    template<class T = char>
    const T* GetFieldDataPtr(size_t ind) const;
    void ReplaceData(TStringView newData) override;

protected:
    TVector<ptrdiff_t> FieldsDataOffsets_;
    TVector<TSkiffData*> Modificated_;

public:
    friend class TSkiffRowFactory;
    friend class TSkiffRowReader;
    friend class TSkiffRowWriter;
};

using TSkiffList = TVector<TSkiffVariant>;  // TODO: Написать кастомный класс с оптимизациями
using TSkiffRow = TSkiffTuple;

class TSkiffRowFactory {
public:
    TSkiffRowFactory(NSkiff::TSkiffSchemaPtr schema) : Schema_(schema) { }
    TSkiffRowFactory(const TSkiffRowFactory& rhs) : Schema_(rhs.Schema_) { }
    TSkiffRowFactory(TSkiffRowFactory&& rhs) {
        *this = std::move(rhs);
    }

    TSkiffRowFactory& operator=(const TSkiffRowFactory& rhs) {
        Schema_ = rhs.Schema_;
        CurrentRow_.reset();

        return *this;
    }
    TSkiffRowFactory& operator=(TSkiffRowFactory&& rhs) {
        Schema_ = rhs.Schema_;
        CurrentRow_ = std::move(rhs.CurrentRow_);

        return *this;
    }

    void StartRow() {
        CurrentRow_ = std::make_unique<TSkiffRow>(Schema_);
        CurrentRow_->FieldsDataOffsets_.clear();
        NestedId_ = -1;
    }
    void AddValue(const char* data, size_t size) {
        size_t currentColumnId = CurrentRow_->FieldsDataOffsets_.size() - (NestedId_ != -1);
        Y_ENSURE(currentColumnId < Schema_->GetChildren().size(), "Unexpected extra field!");

        auto currentColumnSchema = Schema_->GetChildren()[currentColumnId];
        if (NestedId_ == -1) {
            CurrentRow_->FieldsDataOffsets_.push_back(CurrentRow_->Data_.Size());
        } else if (NestedId_ >= 0) {
            currentColumnSchema = currentColumnSchema->GetChildren()[NestedId_];
        }
        
        switch (currentColumnSchema->GetWireType()) {
        case NSkiff::EWireType::Variant8:
        case NSkiff::EWireType::Variant16:
        case NSkiff::EWireType::RepeatedVariant8:
        case NSkiff::EWireType::RepeatedVariant16:
            Y_ENSURE(NestedId_ < 0, "Multi-nested types are not supported.");
            NestedId_ = size == 1 ? *reinterpret_cast<const uint8_t*>(data)
                                    : *reinterpret_cast<const uint16_t*>(data);
            CurrentRow_->Data_.Append(data, size);
            if (NestedId_ == TSkiffVariant::Terminal8Tag()) {
                // TODO: Нужно тут рассматривать отдельно Variant8 и Variant16, т.к. терминальный-8 не терминален для 16
                NestedId_ = -1;
            }
            break;
        case NSkiff::EWireType::Tuple:
            Y_ENSURE(NestedId_ < 0, "Multi-nested types are not supported.");
            NestedId_ = 0;
            AddValue(data, size);
            break;
        
        case NSkiff::EWireType::String32:
        case NSkiff::EWireType::Yson32:
            CurrentRow_->Data_.Append(reinterpret_cast<const char*>(&size), sizeof(size));
        default:
            CurrentRow_->Data_.Append(data, size);

            if (NestedId_ >= 0) {
                currentColumnSchema = Schema_->GetChildren()[currentColumnId];
                switch (currentColumnSchema->GetWireType()) {
                case NSkiff::EWireType::Variant8:
                case NSkiff::EWireType::Variant16:
                    NestedId_ = -1;
                    break;
                case NSkiff::EWireType::RepeatedVariant8:
                case NSkiff::EWireType::RepeatedVariant16:
                    NestedId_ = -2;
                    break;
                case NSkiff::EWireType::Tuple:
                    if (++NestedId_ >= static_cast<int32_t>(currentColumnSchema->GetChildren().size())) {
                        NestedId_ = -1;
                    }
                    break;
                default:
                    ythrow yexception() << "Unexpected case. Simple type cannot be nested.";
                }
            }
        }
    }
    TSkiffRow&& EndRow() {
        return std::move(*(CurrentRow_.release()));
    }

protected:
    NSkiff::TSkiffSchemaPtr Schema_;
    std::unique_ptr<TSkiffRow> CurrentRow_ = nullptr;
    int32_t NestedId_;
};
