## Glossary

### Collection

RDBMS is a DB. Also called a route table. Largest particle size, starting with the largest

collection > table > record

### Table

RDBMS is equivalent to Table. There is only a concept, there is no substance. (There is no real resource in the unit of table) Note that this is just a definition that we will treat it as such a set of data programmatically.

A schema defined in a collection. There are multiple tables in a collection.

### Single Table

A table that has no other tables in its collection. Inevitably, the collection name = table name.

### Table TAG

Tag that uniquely identifies the table within the collection. Same as file name.

### Record

RDBMS is equivalent to Record. Also called an entity. One of the data that exists in the table. Schemas are defined by tables. There are multiple schemas in a collection, but only one schema in a table.

### Reference

RDBMS is equivalent to Relation. There are two main types, ReferenceDynamo and ReferenceMaster. DynamoDB or Master data is distinguished by the difference. Each has a type of Many and One, and there is a difference whether the result of resolving a reference is an array or one. Specifically, Many holds only the HASH key, and One holds the HASH key and the RANGE key together.

### Attribute

Column in RDBMS. Since DynamoDB itself is schemaless, There is no entity defined as a database, only as a concept on Hatsudenki. However, the Hash key and the Range key must be defined as a schema when creating a collection.

### HASH Key

Partition key in RDBMS. PrimaryKey. Must be defined in the collection. An important key to determine the data storage partition. If the hash keys are not well-distributed, the processing will be biased to the partitions, and the efficiency of the capacity unit will be reduced.

### RANGE Key

Sort key in RDBMS. Composite primary key. Some collections are not required and do not exist. In that case, the item is uniquely determined by the hash key. However, it is required for tables designed with the adjacency pattern. When a range key is specified, an item is uniquely determined by a hash key + a range key. High-speed search is possible because sorting is performed based on the contents of the key in the partition.

### Solo Table

A type of table that has only a HASH key. In other words, the HASH key can uniquely identify an item. Due to its characteristics, it is not possible to obtain a list by specifying HASH, so it is not possible to make a Many reference to the Solo table (it can be said exactly, but it has no meaning).

### Multi Table

A type of table that has a HASH key and a RANGE key. Items cannot be identified by the HASH key alone, but a set can be represented by the HASH key. Therefore, it is necessary to specify a HASH key and a RANGE key as a set to specify an item.

### Root Table

The table that is the base of the adjacency pattern. Inheritance source of Child table described later. The Child table to which it belongs contains all the elements defined in the root table.

### Alias Key

The key needed to uniquely identify yourself in the table. The entity is a range key, and is merely a concept of managing a specific range of the range key with an alias.

> - the range key of the root table is `kind`
> - table name is `example_child`
> - alias key is `item_id`, value is `xxxxx`
>
> In the above case, the kind attribute on DynamoDB is It is stored as `example_child@@xxxxx`, and `xxxxx` is returned for the `.item_id` property. that there is no item_id attribute on DynamoDB

### One Cursor

A string that uniquely identifies an item in a table.
In the case of Solo table, it is a HASH key, and in the case of Multi table, it is a character string combining the HASH key and RANGE key.

### Many Cursor

A string representing the set to which the item belongs in the table. Only present in the Multi table, the value is a HASH key. (Because the Solo table does not belong to the set)