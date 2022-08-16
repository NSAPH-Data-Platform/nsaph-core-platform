# NSAPH Core Data Platform

[Documentation Home](https://nsaph-data-platform.github.io/nsaph-platform-docs/home.html)

```{toctree}
---
maxdepth: 4
hidden: 
---
Datamodels
DataLoader
ProjectLoader
TerritorialCodes
SampleQuery
UserRequests
SQLDocumentation
```

```{contents}
---
local:
---
```

## Tool Examples

Examples of tools included in this package are:

* [Universal Data Loader](members/data_loader)
* A [utility to monitor progress of long-running database](members/index) processes like indexing.
* A [utility to infer database schema and generate DDL](members/analyze) from a CSV file
* A [utility to link a table to GIS](members/link_gis) from a CSV file
* A [wrapper around database connection to PostgreSQL](#module-database-connection-wrapper)
* A [utility to import/export JSONLines](members/pg_json_dump) files into/from PostgreSQL
* An [Executor with a bounded queue](members/executors)

## Project Structure

**The package is under intensive development, the project structure is in flux**

Top level directories are:

    - doc
    - resources
    - src

Doc directory contains documentation.

Resource directory contains resources that must be loaded in
the data platform for its normal functioning. For example,
they contain mappings between US states, counties, fips and zip codes.
See details in [Resources](#resources) section.

Src directory contains software source code.
See details in [Software Sources](#software-sources) section.

### Software Sources

The directories under sources are:

    - airflow
    - commonwl
    - html
    - plpgsql
    - python
    - r
    - superset
    - yml

They are described in more details in the corresponding sections.
Here is a brief overview:

* **airflow** contains code and configuration for Airflow.
  Most of the content is deprecated as it is now transferred
  to the deployment package or the specific pipelines. However,
  this directory is intended to contain Airflow plugins that are
  generic for all NSAPH pipelines
* **commonwl** contains reusable workflows, packaged as tools
  that can and should be used by
  all NSAPH pipelines. Examples of such tools
  are: introspection of CSV files, indexing tables, linking
  tables with GIS information for easy mapping, creation of a
  Superset datasource.
* **html** is a deprecated directory for HTML documents
* **plpgsql** contains PostgreSQL procedures and functions
  implemented in PL/pgSQL language
* **python** contains Python code. See [more details](#python-packages).
* **r** contains R utilities. Probably will be deprecated
* **superset** contains definitions of reusable Superset datasets and dashboards
* **yml** contains various YAML files used by the platform.

### Python packages

#### NSAPH Package

This is the main package containing the majority of the code.
Modules and subpackages included in `nsaph` package are described below.

##### Subpackage for Data Modelling

* `nsaph.data_model`

Implements version 2 of the data modelling toolkit.

Version 1 was focused on loading into the database already
processed data saved as flat files. It inferred data
model from the data files structure and accompanied README
files. The inferred data model is converted to database schema
by generating appropriate DDL.

Version 2 focuses on generating code required to do the
actual processing. The main concept is a knowledge domain, or
just a domain. Domain model is define in a YAML file as
described in the [documentation](Datamodels). The main
module that processes the YAML definition of the domain
is [domain.py](members/domain). Another
module, [inserter](members/inserter)
handles parallel insertion of the data into domain tables.

Auxiliary modules perform various maintenance tasks.
Module [index_builder](members/index_builder)
builds indices for a given tables or for all
tables within a domain.
Module [utils](members/utils)
provides convenience function wrappers and defines
class DataReader that abstracts reading CSV and FST files.
In other words, DataReader provides uniform interface
to reading columnar files in two (potentially more)
different formats.

##### Module Database Connection Wrapper

* `nsaph.db`

Module [db](members/db) is a PostgreSQL
connection wrapper. It reads connection parameters from
an `ini` file and connects to the database. It can
transparently connect over **ssh tunnel** when required.

##### Loader Subpackage

* `nsaph.loader`

A set of utilities to manipulate data.

Module [data_loader](members/data_loader)
Implements parallel loading data into a PostgreSQL database.
It is also responsible for loading DDL and creation of view,
both virtual and materialized.

Module [index_builder](members/index_builder)
is a utility to build indices and monitor the build progress.

##### Subpackage to describe and implement user requests [Incomplete]

* `nsaph.requests`

Package `nsaph.requests` contains some code that is
intended to be used for fulfilling user requests. Its
development is currently put on hold.

Module [hdf5_export](members/hdf5_export) exports
result of SQL query as an HDF5 file. The structure of the HDF5 is
described by a YAML request definition.

Module [query](members/query) generates SQL query
from a YAML request definition.

##### Subpackage with miscellaneous utilities

* `nsaph.util`

Package `nsaph.util` contains:

* Support for packaging [resources](#resources)
  in two modules [resources](members/resources)
  and [pg_json_dump](members/pg_json_dump). The
  latter module imports and exports PostgreSQL (pg) tables
  as JSONLines format.
* Module [net](members/net) contains
  one method resolving host to `localhost`. This method is
  required by Airflow.
* Module [executors](members/executors)
  implements a
  [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
  with a bounded queue. It is used to prevent out of memory (OOM)
  errors when processing huge files (to prevent loading
  the whole file into memory before dispatching it for processing).

### YAML files

The majority of files are data model definitions. For now, they
are included in **nsaph** package because they are used by
different utilities and thus, expected to be stored in
a specific location.

Beside data model files, there are YAML files for:

* Conda environments, required for NSAPH pipelines. Unless we
  will be able to create a single environment that accomodate
  all pipelines we will probably deprecate them and move
  into corresponding pipeline repositories.
* Sample user requests for future downstream pipelines
  that create user workspaces from the database. File
  [example_request.yml](members/example_request.yaml) is used by
  [sample request handler](members/hdf5_export)

### Resources

Resources are organized in the following way:

```
- ${database schema}/
    - ddl file for ${resource1}
    - content of ${resource1} in JSON Lines format (*.json.gz)
    - ddl file for ${resource2}
    - content of ${resource2} in JSON Lines format (*.json.gz)
```

Resources can be packaged when a
[wheel](https://pythonwheels.com/)
is built. Support for packaging resources during development and
after a package is deployed is provided by
[resources](members/resources) module.

Another module, [pg_json_dump](members/pg_json_dump),
provides support for packaging tables as resources in JSONLines
format. This format is used natively by some DBMSs.

### SQL Utilities

Utilities, implementing the following:

* [Functions](members/utils.sql):
    * Counting rows in tables
    * Finding a name of the column that contains year from most tables used in data platform
    * Creating a hash for [HLL aggregations](https://en.wikipedia.org/wiki/HyperLogLog)
* Procedure:
    * [A procedure](members/utils.sql) granting `SELECT` privileges
      to a user on all NSAPH tables
    * [A procedure to rename indices](members/rename_indices.sql)
* Set of SQL statements:
    [to map tables from another database](members/map_to_foreign_database.ddl)
    This can be used to map public tables available to anybody
    to a more secure database, containing health data

Utilities used to map territorial codes are described in
[this document](TerritorialCodes)


## Indices and tables

* [genindex](genindex)
* [modindex](modindex)
