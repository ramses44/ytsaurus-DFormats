import random
import json
import string
import argparse
import sys


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


def serialize_yson(data) -> str:
    return json.dumps(data).replace(',', ';').replace(':', ' =').replace('null', '#').replace('true', '%true').replace('false', '%false')


def create_data(schema, file, rows_count):
    rows = [generate_data(schema) for _ in range(rows_count)]
    file.write(serialize_yson(rows)[1:-1])
    del rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('schema', type=str)
    parser.add_argument('rows', type=int, default=1000)
    parser.add_argument('-o', '--output', type=str, required=False)

    args = parser.parse_args()
                
    with open(args.schema) as file:
        schema = eval(file.read())

    if args.output:
        with open(args.output, 'w') as file:
            create_data(schema, file, args.rows)
    else:
        create_data(schema, sys.stdout, args.rows)
    