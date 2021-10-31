NSAPH Data Platform
===================

<!-- toc -->

- [Overview](#overview)
- [Project Structure](#project-structure)
  * [Software Sources](#software-sources)
  * [Python packages](#python-packages)
    + [NSAPH Package](#nsaph-package)
      - [nsaph.data_model](#nsaphdata_model)
      - [Database Connection Wrapper](#database-connection-wrapper)
      - [nsaph.loader](#nsaphloader)
      - [nsaph.requests](#nsaphrequests)
  * [YAML files](#yaml-files)
  * [Resources](#resources)

<!-- tocstop -->

## Overview
The data platform provides generic infrastructure for NSAPH Data Platform
It depends on nsaph_util package but, one can say that it augments it
with APIs and command line utilities dependent on the infrastructure 
environment. For instance, its components assume presence of PostgreSQL
DBMS (version 13 or later) and CWL runtime environment.

Examples of tools included in this package are:

* [Universal Data Loader](doc/Datamodels.md)
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

The package is under intensive development, therefore its 
development branches contain some obsolete modules and utilities.
The project structure can also be in flux.

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

##### nsaph.data_model

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
Module [index2](src/python/nsaph/loader/index_builder.py)
builds indices for a given tables or for all
tables within a domain. 
Module [utils](src/python/nsaph/data_model/utils.py)
provides convenience function wrappers and defines
class DataReader that abstracts reading CSV and FST files.
In other words, DataReader provides uniform interface
to reading columnar files in two (potentially more)
different formats.

##### Database Connection Wrapper

Module [db](src/python/nsaph/db.py) is a PostgreSQL
connection wrapper. It reads connection parameters from
an `ini` file and connects to the database. It can
transparently connect over _**ssh tunnel**_ when required.

##### nsaph.loader

A wrapper around the 
[Universal Data Loader](doc/Datamodels.md). 


##### nsaph.requests
Package nsaph.requests contains some code that is
intended to be used for fulfilling user requests. Its 
development is currently put on hold.

Module [hdf5_export](src/python/nsaph/requests/hdf5_export.py) exports
result of SQL query as an HDF5 file. The structure of the HDF5 is
described by a YAML request definition. 

Module [query](src/python/nsaph/requests/query.py) generates SQL query
from a YAML request definition.

##### nsaph.util

Package nsaph.util contains: 

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

##### nsaph.tools

This package contains code that was written to try to extract
corrupted medicare data for 2015. Ultimately, this attempt
was unsuccessful.

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
