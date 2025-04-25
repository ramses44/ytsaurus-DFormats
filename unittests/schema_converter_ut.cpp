#include <library/cpp/testing/unittest/registar.h>
#include <dformats/skiff_reader.h>

Y_UNIT_TEST_SUITE(Skiff) {
    Y_UNIT_TEST(TestIntegers) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "int8")("type", "int8")("required", true))
            .Add(TNode()("name", "uint8")("type", "uint8")("required", true))
            .Add(TNode()("name", "int16")("type", "int16")("required", true))
            .Add(TNode()("name", "uint16")("type", "uint16")("required", true))
            .Add(TNode()("name", "int32")("type", "int32")("required", true))
            .Add(TNode()("name", "uint32")("type", "uint32")("required", true))
            .Add(TNode()("name", "int64")("type", "int64")("required", true))
            .Add(TNode()("name", "uint64")("type", "uint64")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 8);
        
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Int8);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Uint8);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[2]->GetWireType(), NSkiff::EWireType::Int16);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[3]->GetWireType(), NSkiff::EWireType::Uint16);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[4]->GetWireType(), NSkiff::EWireType::Int32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[5]->GetWireType(), NSkiff::EWireType::Uint32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[6]->GetWireType(), NSkiff::EWireType::Int64);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[7]->GetWireType(), NSkiff::EWireType::Uint64);
    }

    Y_UNIT_TEST(TestFractional) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "float")("type", "float")("required", true))
            .Add(TNode()("name", "double")("type", "double")("required", true))
            .Add(TNode()("name", "decimal")("type_v3", TNode()
                    ("type_name", "decimal")
                    ("precision", 10)
                    ("scale", 2)
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 3);
        
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Double);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Double);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[2]->GetWireType(), NSkiff::EWireType::Double);
        // Для Decimal, наверное, потом нужно сделать не Double, т.к. у него вообще плавающая битность (32-256)
        // https://ytsaurus.tech/docs/ru/user-guide/storage/data-types#schema_decimal
    }

    Y_UNIT_TEST(TestString) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "string")("type", "string")("required", true))
            .Add(TNode()("name", "utf8")("type", "utf8")("required", true))
            .Add(TNode()("name", "json")("type", "json")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 3);
        
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::String32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::String32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[2]->GetWireType(), NSkiff::EWireType::String32);
    }

    Y_UNIT_TEST(TestTuple) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "int32")("type", "int32")("required", true))
            .Add(TNode()("name", "tuple")("type_v3", TNode()
                    ("type_name", "tuple")
                    ("elements", TNode()
                        .Add(TNode()("type", "string"))
                        .Add(TNode()("type", "int32"))
                    )
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Int32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Tuple);

        skiffColumns = skiffColumns[1]->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::String32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Int32);
    }

    Y_UNIT_TEST(TestVariant) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "int32")("type", "int32")("required", true))
            .Add(TNode()("name", "variant")("type_v3", TNode()
                    ("type_name", "variant")
                    ("elements", TNode()
                        .Add(TNode()("type", "string"))
                        .Add(TNode()("type", "int32"))
                    )
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Int32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Variant8);

        auto skiffVariants = skiffColumns[1]->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants[0]->GetWireType(), NSkiff::EWireType::String32);
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants[1]->GetWireType(), NSkiff::EWireType::Int32);
    }

    Y_UNIT_TEST(TestBoolean) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "boolean")("type", "boolean")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Boolean);
    }

    Y_UNIT_TEST(TestTimes) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "date32")("type", "date32")("required", true))
            .Add(TNode()("name", "datetime64")("type", "datetime64")("required", true))
            .Add(TNode()("name", "timestamp64")("type", "timestamp64")("required", true))
            .Add(TNode()("name", "interval64")("type", "interval64")("required", true))
            .Add(TNode()("name", "date")("type", "date")("required", true))
            .Add(TNode()("name", "datetime")("type", "datetime")("required", true))
            .Add(TNode()("name", "timestamp")("type", "timestamp")("required", true))
            .Add(TNode()("name", "interval")("type", "interval")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 8);
        
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Uint32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[1]->GetWireType(), NSkiff::EWireType::Uint64);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[2]->GetWireType(), NSkiff::EWireType::Uint64);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[3]->GetWireType(), NSkiff::EWireType::Uint64);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[4]->GetWireType(), NSkiff::EWireType::Uint16);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[5]->GetWireType(), NSkiff::EWireType::Uint32);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[6]->GetWireType(), NSkiff::EWireType::Uint64);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[7]->GetWireType(), NSkiff::EWireType::Uint64);
    }

    Y_UNIT_TEST(TestUuid) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "uuid")("type", "uuid")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Uint128);
    }

    Y_UNIT_TEST(TestOptional) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "optional")("type_v3", TNode()
                ("type_name", "optional")
                ("item", "bool")
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Variant8);

        auto skiffVariants = skiffColumns[0]->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants[0]->GetWireType(), NSkiff::EWireType::Nothing);
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants[1]->GetWireType(), NSkiff::EWireType::Boolean);
    }

    Y_UNIT_TEST(TestList) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "list")("type_v3", TNode()
                ("type_name", "list")
                ("item", "double")
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::RepeatedVariant8);

        auto skiffVariants = skiffColumns[0]->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffVariants[0]->GetWireType(), NSkiff::EWireType::Double);
    }

    Y_UNIT_TEST(TestStruct) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "struct")("type_v3", TNode()
                ("type_name", "struct")
                ("members", TNode()
                    .Add(TNode()("name", "foo")("type", "int32"))
                    .Add(TNode()("name", "bar")("type", "string"))
                )
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Yson32);
    }

    Y_UNIT_TEST(TestDict) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "dict")("type_v3", TNode()
                ("type_name", "dict")
                ("key", "int64")
                ("value", "string")
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Yson32);
    }

    Y_UNIT_TEST(TestYson) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "yson")("type", "any")("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Yson32);
    }

    Y_UNIT_TEST(TestTagged) {
        auto testTableSchema = TTableSchema::FromNode(TNode()
            .Add(TNode()("name", "tagged")("type_v3", TNode()
                ("type_name", "tagged")("tag", "image/svg")("item", "string")    
            )("required", true))
        );
        auto convertedSkiffSchema = SkiffSchemaFromTableSchema(testTableSchema);

        UNIT_ASSERT_VALUES_EQUAL(convertedSkiffSchema->GetWireType(), NSkiff::EWireType::Tuple);

        auto skiffColumns = convertedSkiffSchema->GetChildren();
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(skiffColumns[0]->GetWireType(), NSkiff::EWireType::Yson32);
    }
}