import random
import json
import argparse
import os

PROTO_HEADER = 'import "yt/yt_proto/yt/formats/extension.proto";\n'

NUMERIC_YT_TYPES = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float', 'double']
BOOL_YT_TYPES = ['boolean']
STRING_YT_TYPES = ['string', 'utf8']

TYPE_MAPPING_PROTO = {
    "uint64": "uint64",
    "uint32": "uint32",
    "uint16": "uint32",
    "uint8": "uint32",
    "int64": "int64",
    "int32": "int32",
    "int16": "int32",
    "int8": "int32",
    "double": "double",
    "float": "float",
    "boolean": "bool",
    "string": "string",
    "utf8": "string",
}


def generate_schema(columns_count: int, types: list, allow_optional: bool = False) -> list[dict]:
    return [{"name": f"column_{i + 1}", "type": random.choice(types),
             "required": random.random() < 0.5 if allow_optional else True} for i in range(columns_count)]


def generate_proto_file(schema: list[dict], message_name: str) -> str:
    proto = f'\nmessage {message_name} {"{"}\n'

    for i, field in enumerate(schema, 1):
        proto_type = TYPE_MAPPING_PROTO.get(field["type"], "bytes")
        required = "required" if field["required"] else "optional"
        proto += f'    {required} {proto_type} {field["name"]} = {i} '
        proto += f'[(NYT.column_name) = "{field["name"]}"];\n'
    
    proto += "}\n"
    return proto


def serialize_yson(data) -> str:
    return json.dumps(data).replace(',', ';').replace(':', ' =').replace('null', '#').replace('true', '%true').replace('false', '%false')


def create_thousand_numeric_schema(output_dir):
    schema = generate_schema(1000, NUMERIC_YT_TYPES)

    proto = generate_proto_file(schema, 'TThousandNumericMessage')
    with open(os.path.join(output_dir, 'bench.proto'), 'a') as file:
        file.write(proto)
    del proto

    with open(os.path.join(output_dir, 'thousand_numeric_schema.py'), 'w') as file:
        file.write(str(schema))

    with open(os.path.join(output_dir, 'thousand_numeric_schema.txt'), 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_simple_ten_schema(output_dir):
    schema = generate_schema(10, NUMERIC_YT_TYPES + BOOL_YT_TYPES + STRING_YT_TYPES)

    proto = generate_proto_file(schema, 'TSimpleTenMessage')
    with open(os.path.join(output_dir, 'bench.proto'), 'a') as file:
        file.write(proto)
    del proto

    with open(os.path.join(output_dir, 'simple_ten_schema.py'), 'w') as file:
        file.write(str(schema))

    with open(os.path.join(output_dir, 'simple_ten_schema.txt'), 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_simple_two_schema(output_dir):
    schema = [{"name": "id", "type": "uint32", "required": True},
              {"name": "data", "type": "string", "required": True}]

    proto = generate_proto_file(schema, 'TSimpleTwoMessage')
    with open(os.path.join(output_dir, 'bench.proto'), 'a') as file:
        file.write(proto)
    del proto

    with open(os.path.join(output_dir, 'simple_two_schema.py'), 'w') as file:
        file.write(str(schema))

    with open(os.path.join(output_dir, 'simple_two_schema.txt'), 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_complex_types_schema(output_dir):
    schema = [{"name": "optional", "type_v3": {"type_name": "optional", "item": "string"}},
              {"name": "list", "type_v3": {"type_name": "list", "item": "double"}, "required": True},
              {"name": "struct", "type_v3": {"type_name": "struct", "members": [{"name": "foo", "type": "int32"}, {"name": "bar", "type": "string"}]}, "required": True},
              # {"name": "tuple", "type_v3": {"type_name": "tuple", "elements": [{"type": "double"}, {"type": "double"}]}, "required": True},
              # {"name": "variant", "type_v3": {"type_name": "variant", "elements": [{"type": "int32"}, {"type": "string"}, {"type": "double"}]}, "required": True},
              {"name": "dict", "type_v3": {"type_name": "dict", "key": "int64", "value": "string"}, "required": True}]

    with open(os.path.join(output_dir, 'bench.proto'), 'a') as file:
        file.write("""
message TComplexTypesMessage {
    option (NYT.default_field_flags) = SERIALIZATION_YT;
    option (NYT.default_field_flags) = MAP_AS_DICT;

    message TStructMessage {
        required int32 foo = 1 [(NYT.column_name) = "foo"];
        required string bar = 2 [(NYT.column_name) = "bar"];
    }

    message TListMessage {
        option (NYT.default_field_flags) = SERIALIZATION_YT;
        repeated double list = 1 [(NYT.column_name) = "list"];
    }

    optional string optional = 1 [(NYT.column_name) = "optional"];
    required TListMessage list = 2 [(NYT.flags) = EMBEDDED];
    required TStructMessage struct = 3 [(NYT.column_name) = "struct"];
    map<int64, string> dict = 4 [(NYT.column_name) = "dict"];
}
""")

    with open(os.path.join(output_dir, 'complex_types_schema.py'), 'w') as file:
        file.write(str(schema))

    with open(os.path.join(output_dir, 'complex_types_schema.txt'), 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--scenario', type=str, required=False)
    parser.add_argument('-o', '--outdir', type=str, default='.')

    args = parser.parse_args()

    with open(os.path.join(args.outdir, 'bench.proto'), 'w') as file:
        file.write(PROTO_HEADER)

    if args.scenario == "ThousandNumeric" or not args.scenario:
        create_thousand_numeric_schema(args.outdir)
    if args.scenario == "SimpleTen" or not args.scenario:
        create_simple_ten_schema(args.outdir)
    if args.scenario == "SimpleTwo" or not args.scenario:
        create_simple_two_schema(args.outdir)
    if args.scenario == "ComplexTypes" or not args.scenario:
        create_complex_types_schema(args.outdir)
