NSAPH Data Platform
===================
                                
[Full Platform Documentation](https://nsaph-data-platform.github.io/nsaph-platform-docs/home.html)

<!-- toc -->

- [Overview](#overview)
- [Tool Examples](#tool-examples)
- [Project Structure](#project-structure)
  * [Software Sources](#software-sources)
  * [Python packages](#python-packages)
    + [NSAPH Package](#nsaph-package)
      - [Subpackage for Data Modelling](#subpackage-for-data-modelling)
      - [Module Database Connection Wrapper](#module-database-connection-wrapper)
      - [Loader Subpackage](#loader-subpackage)
      - [Subpackage to describe and implement user requests [Incomplete]](#subpackage-to-describe-and-implement-user-requests-incomplete)
      - [Subpackage with miscellaneous utilities](#subpackage-with-miscellaneous-utilities)
  * [YAML files](#yaml-files)
  * [Resources](#resources)

<!-- tocstop -->

## Overview
The data platform provides generic infrastructure for NSAPH Data Platform
It depends on nsaph_util package, but it augments it
with APIs and command line utilities dependent on the infrastructure 
and the environment. For instance, its components assume presence of PostgreSQL
DBMS (version 13 or later) and CWL runtime environment.

The package is under intensive development, therefore its 
development branches contain some obsolete modules and utilities.
The project structure can also be in flux.

## Tool Examples
Examples of tools included in this package are:

* [Universal Data Loader](src/python/nsaph/loader/data_loader.py)
* A [utility to monitor progress of long-running database](src/python/nsaph/index.py)
    processes like indexing.
* A [utility to infer database schema and generate DDL](src/python/nsaph/analyze.py)
    from a CSV file
* A [utility to link a table to GIS](src/python/nsaph/link_gis.py)
  from a CSV file
* A [wrapper around database connection to PostgreSQL](#database-connection-wrapper)
* A [utility to import/export JSONLines](src/python/nsaph/util/pg_json_dump.py)
    files into/from PostgreSQL
* An [Executor with a bounded queue](src/python/util/executors.py)

## Project Structure

**The package is under intensive development, the
project structure is in flux**

Top level directories are:

    - doc
    - resources
    - src

Doc directory contains documentation.

Resource directory contains resources that must be loaded in 
the data platform for its normal functioning. For example,
they contain mappings between US states, counties, fips and zip codes.
See details in [Resources](#Resources) section.

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

* _**airflow**_ contains code and configuration for Airflow. 
  Most of the content is deprecated as it is now transferred
  to the deployment package or the specific pipelines. However,
  this directory is intended to contain Airflow plugins that are
  generic for all NSAPH pipelines
* **_commonwl_** contains reusable workflows, packaged as tools 
    that can and should be used by
    all NSAPH pipelines. Examples of such tools
    are: introspection of CSV files, indexing tables, linking
    tables with GIS information for easy mapping, creation of a 
    Superset datasource.
* **_html_** is a deprecated directory for HTML documents
* **_plpgsql_** contains PostgreSQL procedures and functions
    implemented in PL/pgSQL language
* **_python_** contains Python code. See [more details](#python-packages).
* **_r_** contains R utilities. Probably will be deprecated
* **_superset_** contains definitions of reusable Superset 
    datasets and dashboards
* **_yml_** contains various YAML files used by the platform.

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
described in the [documentation](doc/Datamodels.md). The main
module that processes the YAML definition of the domain
is [domain.py](src/python/nsaph/data_model/domain.py). Another
module, [inserter](src/python/nsaph/data_model/inserter.py)
handles parallel insertion of the data into domain tables.

Auxiliary modules perform various maintenance tasks. 
Module [index_builder](src/python/nsaph/loader/index_builder.py)
builds indices for a given tables or for all
tables within a domain. 
Module [utils](src/python/nsaph/data_model/utils.py)
provides convenience function wrappers and defines
class DataReader that abstracts reading CSV and FST files.
In other words, DataReader provides uniform interface
to reading columnar files in two (potentially more)
different formats.

##### Module Database Connection Wrapper

* `nsaph.db`

Module [db](src/python/nsaph/db.py) is a PostgreSQL
connection wrapper. It reads connection parameters from
an `ini` file and connects to the database. It can
transparently connect over _**ssh tunnel**_ when required.

##### Loader Subpackage

* `nsaph.loader`

A set of utilities to manipulate data.

Module [data_loader](src/python/nsaph/loader/data_loader.py) 
Implements parallel loading data into a PostgreSQL database.
It is also responsible for loading DDL and creation of view, 
both virtual and materialized.

Module [index_builder](src/python/nsaph/loader/index_builder.py)
is a utility to build indices and monitor the build progress.


##### Subpackage to describe and implement user requests [Incomplete]

* `nsaph.requests`

Package `nsaph.requests` contains some code that is
intended to be used for fulfilling user requests. Its 
development is currently put on hold.

Module [hdf5_export](src/python/nsaph/requests/hdf5_export.py) exports
result of SQL query as an HDF5 file. The structure of the HDF5 is
described by a YAML request definition. 

Module [query](src/python/nsaph/requests/query.py) generates SQL query
from a YAML request definition.

##### Subpackage with miscellaneous utilities

* `nsaph.util`

Package `nsaph.util` contains: 

* Support for packaging [resources](#resources)
  in two modules [resources](src/python/nsaph/util/resources.py) 
  and [pg_json_dump](src/python/nsaph/util/pg_json_dump.py). The 
  latter module imports and exports PostgreSQL (pg) tables
  as JSONLines format.
* Module [net](src/python/nsaph/util/net.py) contains
  one method resolving host to `localhost`. This method is
  required by Airflow.
* Module [executors](src/python/nsaph/util/executors.py) 
  implements a  
  [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
  with a bounded queue. It is used to prevent out of memory (OOM)
  errors when processing huge files (to prevent loading
  the whole file into memory before dispatching it for processing).

### YAML files
The majority of files are data model definitions. For now, they 
are included in **_nsaph_** package because they are used by 
different utilities and thus, expected to be stored in 
a specific location.

Beside data model files, there are YAML files for:

* Conda environments, required for NSAPH pipelines. Unless we
  will be able to create a single environment that accomodate 
  all pipelines we will probably deprecate them and move 
  into corresponding pipeline repositories. 
* Sample user requests for future downstream pipelines
  that create user workspaces from the database. File 
  [example_request.yml](src/yml/example_request.yaml) is used by
  [sample request handler](src/python/nsaph/requests/hdf5_export.py)

### Resources

Resources are organized in the following way:

    - ${database schema}/
        - ddl file for ${resource1}
        - content of ${resource1} in JSON Lines format (*.json.gz)
        - ddl file for ${resource2}
        - content of ${resource2} in JSON Lines format (*.json.gz)

Resources can be packaged when a 
[wheel](https://pythonwheels.com/) 
is built. Support for packaging resources during development and
after a package is deployed is provided by 
[resources](src/python/nsaph/util/resources.py) module.

Another module, [pg_json_dump](src/python/nsaph/util/pg_json_dump.py),
provides support for packaging tables as resources in JSONLines
format. This format is used natively by some DBMSs.

### SQL and PL/pgSQL Utilities

Utilities, implementing the following: 

* [Functions](src/sql/utils.sql):
    * Counting rows in tables
    * Finding a name of the column that contains year from most tables used
        in data platform
    * Creating a hash for [HLL aggregations](https://en.wikipedia.org/wiki/HyperLogLog)
* Procedure:
    * [A procedure](src/sql/utils.sql)) granting `SELECT` privileges 
      to a user on all NSAPH tables
    * [A procedure to rename indices](src/sql/rename_indices.sql)
* Set of SQL statements 
    [to map tables from another database](src/sql/map_to_foreign_database.ddl)
    This can be used to map public tables available to anybody
    to a more secure database, containing health data

Utilities used to map territorial codes are described in 
[this document](doc/TerritorialCodes.md)
