import random
import json
import string

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


def generate_value(type: str):
    match type:
        case 'int8':
            return random.randrange(-2**7, 2**7)
        case 'int16':
            return random.randrange(-2**15, 2**15)
        case 'int32':
            return random.randrange(-2**31, 2**31)
        case 'int64':
            return random.randrange(-2**63, 2**63)
        case 'uint8':
            return random.randrange(2**7)
        case 'uint16':
            return random.randrange(2**15)
        case 'uint32':
            return random.randrange(2**31)
        case 'uint64':
            return random.randrange(2**63)
        case 'float':
            return round(random.randrange(2**32) / random.randrange(2**32), 4)
        case 'double':
            return round(random.randrange(2**64) / random.randrange(2**64), 8)
        case 'boolean':
            return random.random() < 0.5
        case 'string' | 'utf8':
            return ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1, 50)))
        case _:
            raise ValueError(f"Unknown type {type}")


def generate_data(schema: list[dict]) -> dict:
    data = {}

    for column in schema:
        if not column.get('required', False) and random.random() < 0.1:
            data[column['name']] = None
            continue

        match column.get('type', column.get('type_v3', {'type_name': None})['type_name']):
            case 'optional':
                data[column['name']] = generate_value(column['type_v3']['item']) if random.random() > 0.1 else None
            case 'list':
                data[column['name']] = [generate_value(column['type_v3']['item']) for _ in range(random.randint(1, 50))]
            case 'struct':
                data[column['name']] = {member['name']: generate_value(member['type']) for member in column['type_v3']['members']}
            case 'tuple':
                data[column['name']] = [generate_value(element['type']) for element in column['type_v3']['elements']]
            case 'variant':
                tag = random.randrange(len(column['type_v3']['elements']))
                data[column['name']] = [tag, generate_value(column['type_v3']['elements'][tag]['type'])]
            case 'dict':
                data[column['name']] = [[generate_value(column['type_v3']['key']), 
                                        generate_value(column['type_v3']['value'])] for _ in range(random.randint(1, 50))]
            case _:
                data[column['name']] = generate_value(column['type'])
            
    return data

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


def create_thousand_numeric():
    schema = generate_schema(1000, NUMERIC_YT_TYPES)

    proto = generate_proto_file(schema, 'TThousandNumericMessage')
    with open('dformats/benchmarks/bench.proto', 'a') as file:
        file.write(proto)
    del proto
    
    rows = [generate_data(schema) for _ in range(1000)]
    with open('../thousand_numeric_data.txt', 'w') as file:
        file.write(serialize_yson(rows)[1:-1])
    del rows

    with open('../thousand_numeric_schema.txt', 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_simple_ten():
    schema = generate_schema(10, NUMERIC_YT_TYPES + BOOL_YT_TYPES + STRING_YT_TYPES)

    proto = generate_proto_file(schema, 'TSimpleTenMessage')
    with open('dformats/benchmarks/bench.proto', 'a') as file:
        file.write(proto)
    del proto
    
    rows = [generate_data(schema) for _ in range(1000)]
    with open('../simple_ten_data.txt', 'w') as file:
        file.write(serialize_yson(rows)[1:-1])
    del rows

    with open('../simple_ten_schema.txt', 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_simple_two():
    schema = [{"name": "id", "type": "uint32", "required": True},
              {"name": "data", "type": "string", "required": True}]

    proto = generate_proto_file(schema, 'TSimpleTwoMessage')
    with open('dformats/benchmarks/bench.proto', 'a') as file:
        file.write(proto)
    del proto
    
    rows = [generate_data(schema) for _ in range(1000)]
    with open('../simple_two_data.txt', 'w') as file:
        file.write(serialize_yson(rows)[1:-1])
    del rows

    with open('../simple_two_schema.txt', 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


def create_complex_types():
    schema = [{"name": "optional", "type_v3": {"type_name": "optional", "item": "string"}},
              {"name": "list", "type_v3": {"type_name": "list", "item": "double"}, "required": True},
              {"name": "struct", "type_v3": {"type_name": "struct", "members": [{"name": "foo", "type": "int32"}, {"name": "bar", "type": "string"}]}, "required": True},
              {"name": "tuple", "type_v3": {"type_name": "tuple", "elements": [{"type": "double"}, {"type": "double"}]}, "required": True},
              {"name": "variant", "type_v3": {"type_name": "variant", "elements": [{"type": "int32"}, {"type": "string"}, {"type": "double"}]}, "required": True},
              {"name": "dict", "type_v3": {"type_name": "dict", "key": "int64", "value": "string"}, "required": True}]
    
    rows = [generate_data(schema) for _ in range(1000)]
    with open('../complex_types_data.txt', 'w') as file:
        file.write(serialize_yson(rows)[1:-1])
    del rows

    with open('../complex_types_schema.txt', 'w') as file:
        file.write('<schema = ')
        file.write(serialize_yson(schema))
        file.write('>')


if __name__ == '__main__':
    with open('dformats/benchmarks/bench.proto', 'w') as file:
        file.write(PROTO_HEADER)

    create_thousand_numeric()
    create_simple_ten()
    create_simple_two()
    create_complex_types()