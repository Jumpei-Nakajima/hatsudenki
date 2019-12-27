# HATSUDENKI

DynamoDB ORM implemented in Python.
Supports AsyncIO and includes a code generator.

日本人なんで英語が苦手なんだ、すまんな。

## features

- declarative schema management with YAML
- fully support Adjacency List Design Pattern
- automatic code generation
- CRUD functions
- simple filter by django-like operator
- automatic query construction
- batch operation (get & put)
- transaction operation
- relation by application layer implementation
- manage in-memory fixed data. (excel automatic generation for input)
- table migration

## Getting started

- execute `hatsudenki-gen setup_dsl`(created `dsl` directory)
- write table definition to yaml
- execute code generator (ex: `hatsudenki-gen genereate -dsl dsl -o out`)
- Import the generated code and start using!

## Basic Example

### basic usage

```python
# get 1 record
item = await Example.get(name="xxxxx", age=1)
# get records by hash_key (limit 10)
items = await Example.get_list(name="xxxxx", limit=10)
# get by query dict (name="xxxxx" & age=1)
item = await Example.query({"name": "xxxxx", "age": 1})
# use django-like operator (ex: gte)
items = await Example.query_list({"name": "xxxxx", "age__gte": 10})

# put record to table
item = Example(name="xxxxx", age=1)
await item.put()
# force overwrite
await item.put(overwrite=True)
# update record
item.number_value = 100
await item.update()
# upsert
await item.update(upsert=True)
# create if it does not exist
item = await Example.get_or_create("xxxxx", 1)
```

### example schema yaml

```yaml
# attribute definition block
attributes:
  user_id: # key name
    hash: true # hash key if true
    type: uuid # value type
    comment: unique user id # option, Not used for processing
  range_key_value:
  	range: true # range key if true
  	type: string
  gsi_hash_value:
  	type: number
  gsi_range_value:
  	type: number
  str_value:
    type: string

# index definition block
indexes:
	- type: global # global = GSI / local = LSI
	  hash: gsi_hash_value # hash key name(not required when LSI)
	  range: gsi_range_value # range key name(required when LSI. option when GSI)
    # index capacity units(not required when LSI, not required when ONDEMAND mode)
    capacity_units:
      read: 1
      write: 1
# table capacity units definition block
capacity_units:
  # ONDEMAND mode when read=write=1
  read: 1
  write: 1
```

## Requirements

- python 3.7+
- aiobotocore
- openpyxl