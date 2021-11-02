# Data Modelling for NSAPH Data Platform 
**How data models are defined and handled**

<!-- toc -->

- [Introduction](#introduction)
- [Domain](#domain)
- [Table](#table)
  * [Invalid Record](#invalid-record)
- [Column](#column)
  * [Source](#source)
  * [Generated columns](#generated-columns)
  * [Computed Columns](#computed-columns)
  * [Transposing Columns](#transposing-columns)
- [Generation of the database schema (DDL)](#generation-of-the-database-schema-ddl)
- [Indexing Policies](#indexing-policies)
- [Ingesting Data](#ingesting-data)

<!-- tocstop -->

## Introduction

Data models consist of database tables, relations between them
(e.g. foreign keys), indices and conventions that govern things 
like namings and roles of specific columns.

We assume that a model is defined for a specific knowledge domain. 
Between domains, data can be linked based on the naming conventions 
for columns. For instance, a column named `zipcode` means the US zip
code in any domain and thus can be used for linkages and aggregations.

Currently, we are in the process of defining data models for the 
following domains
* Medicaid
* EPA
* Census
* Gridmet

## Domain

Handling domains is implemented by the 
[Domain](../src/python/nsaph/data_model/domain.py) class.

For each domain, its data model is defined by a YAML file in 
the following directory: [src/yml](../src/yml)

Each model is represented by a "forest": a set of treelike 
structures of tables. It can contain one or several root tables

Domain should be the first entry in the YAML file:

    my_domain:

The following parameters can be defined fro domain:

| Parameter | Required? | Description |
|-----------|-----------|-------------|
|schema | yes | Database schema, in which all tables are generated
|schema.audit| no | Database schema for tables containing audit logs of data ingestion, including corrupted, duplicate and inconsistent records |
|index | no | Default indexing policy for this domain. This policy is used for tables that do not define their own indexing policy |
|tables | yes | list of table definitions |
|description | no | description of this domain to be included in auto-generated documentation
|reference | no | URL with external documentation
|header | no | Boolean value, passed to CSV loader. Describes input source rather than data model itself
|quoting| no | Numeric value, passed to CSV loader. Describes input source rather than data model itself (QUOTE_MINIMAL=0, QUOTE_ALL=1, QUOTE_NONNUMERIC=2, QUOTE_NONE=3) 

## Table 

The following parameters can be defined for a table:

| Parameter | Required? | Description |
|-----------|-----------|-------------|
|type | no | view / table |
|hard_linked | no | Denotes that the table is an integral part of parent table rather than a separate table with a many-to-one relationship to the parent table|
|columns | yes | list of column definitions |
|primary_key | yes | list of column names included in the table primary key |
|children | no | list of table definitions for child tables of this table|
|description | no | description of this domain to be included in auto-generated documentation
|reference | no | URL with external documentation
|invalid.records | no | [action](#invalid-record) to be performed upon encountering an invalid record (corrupted, incomplete, duplicate, etc.) |

### Invalid Record

By default, an exception is raised if an invalid
record is encountered during data ingestion. However,
it is possible to override this behaviour by instructing
the data loader to either ignore such records or put them
in a special audit table.

| Parameter | Required? | Description |
|-----------|-----------|-------------|
|action | yes | Action to be performed: `INSERT` or `IGNORE` |
|target | yes/no | For action INSERT - a target table|
|description | no | description of this domain to be included in auto-generated documentation
|reference | no | URL with external documentation

## Column

| Parameter | Required? | Description |
|-----------|-----------|-------------|
|type | yes | Database type |
|source | no | [source](#source) of the data|
|description | no | description of this domain to be included in auto-generated documentation
|reference | no | URL with external documentation

Beside "normal" columns, when the value is 
directly taken from a column in a tabular input source,
there are three types of special columns:

* Computed columns
* Generated columns
* Transposed columns (i.e., when multiple 
     columns of a single record are converted to multiple
     records)

Special columns must have their `source` defined. If a column 
name in input source is different from a column name in the 
database, such column also must define `source`.

### Source

Source must be defined for special columns and for columns
with the name in the database different from the name
in the input source.

| Parameter | Required? | Description |
|-----------|-----------|-------------|
|column name | no | A column name in the incoming tabular data |
|type | no | Types: `generated`, `multi_column`, `range`, `compute`|
|range | no | |
|code | no | Code for generated and computed columns|


### Generated columns

### Computed Columns

### Transposing Columns 





## Generation of the database schema (DDL)

From a domain YAML file, the database schema is 
generated in the form of PostgreSQL dialect of DDL.

The main class responsible for the generation of DDL is
[Domain](../src/python/nsaph/data_model/domain.py#Domain)

## Indexing Policies

* **explicit**. Indices are only created for columns that
    define an index
* **all** Indices are only created for all columns
* **selected** Indices are only created for columns 
     matching certain pattern (defined in `index_columns` variable
     of [model](../src/python/nsaph/data_model/model.py)) module
* **unless excluded** Indices are only created for all columns 
  not explicitly excluded

## Ingesting Data 

The following command ingests data into a table and all hard-linked
child tables:

    usage: python -u -m nsaph.data_model.model2 [-h] [--domain DOMAIN] 
                [--table TABLE] [--data DATA]
                [--reset] [--autocommit] [--db DB] [--connection CONNECTION]
                [--page PAGE] [--log LOG] [--limit LIMIT] [--buffer BUFFER]
                [--threads THREADS]

    optional arguments:
    -h, --help              show this help message and exit
    --domain DOMAIN         Name of the domain
    --table TABLE, -t TABLE Name of the table to load data into
    --data DATA             Path to a data file or directory. Can be a single CSV,
                            gzipped CSV or FST file or a directory recursively
                            containing CSV files. Can also be a tar, tar.gz (or
                            tgz) or zip archive containing CSV files
    --pattern PATTERN       pattern for files in a directory or an archive, e.g.
                            "**/maxdata_*_ps_*.csv"
    --reset                 Force recreating table(s) if it/they already exist
    --incremental           Commit every file and skip over files that have
                            already been ingested
    --autocommit            Use autocommit
    --db DB                 Path to a database connection parameters file
    --connection CONNECTION Section in the database connection parameters file
    --page PAGE             Explicit page size for the database
    --log LOG               Explicit interval for logging
    --limit LIMIT           Load at most specified number of records
    --buffer BUFFER         Buffer size for converting fst files
    --threads THREADS       Number of threads writing into the database

