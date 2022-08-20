# Python Core Platform Components 

```{seealso}
[Python Module Index](modindex)
```
 
## Package nsaph

* [](members/nsaph.rst) API to initialize logging and database connections
* [db](members/db.rst) Database connections API
* [fips](members/fips.rst) US State FIPS codes, represented as Python dictionary
* [pg_keywords](members/pg_keywords.rst) PostgreSQL keywords, e.g.
  type names, etc.

## Package nsaph.data_model
                                         
APIs for data modelling, loading and manipulations

* [Domain class](members/domain.rst) Data modeling for a data domain
* [Inserter](members/inserter) API to parallel data loader
* [utils](members/utils) Miscellaneous data handling API

## Package nsaph.loader
         
Command line utilities to manipulate data

* [](members/loader.rst)
* [Data Introspector](members/introspector) introspects columnar data to 
  infer the types of the columns
* [](members/data_loader.rst) Transfers data from file(s) to the database.
    Implements parallel loading and handling multiple file formats
  (CSV, FST, JSON, SAS, FWF)
* [Project Loader](members/project_loader.rst) Recursively introspects and loads 
    data directory into the database
* [Index Builder](members/index_builder) Builds indices for a table or for all 
    tables in the data domain, reporting the progress
* [Database Activity Monitor](members/monitor) API and command line utility
    to monitor database activity
* [Vacuum](members/vacuum) Executes VACUUM command in the database to tune
    tables for better query performance
* [Common Configuration](members/common) Common configuration options
    for working with the database
* [Loader Configurator](members/loader_config) Configuration options
    for data loader


## Package nsaph.requests

Attempt to tackle User requests API. Incomplete work in progress

* [HDF5 Export](members/hdf5_export.rst) API and utility to export result 
    of quering the database in HDF5 format
* [Query class](members/query) API and utility to generate SQL query 
    based on a YAML query specification

## Package nsaph.utils

Miscellaneous tools and APIs

* [CWL Output collector](members/cwl_collect_outputs) Command line 
    utility to copy outputs from a CWL tool or sub-workflow into 
    the calling workflow
* [Blocking Executor Pool](members/executors) Blocking executor pool to 
    avoid memory overflow with long queues
* [Localhost for Airflow](members/net) Overrides host resolver for Airflow, so
    it can use localhost
* [JSON to/from Table converter](members/pg_json_dump) A command line utility 
    to import/export JSON files as database tables
* [Resource finder](members/resources) An API to look for resources
    in Python packages
* [sas_explorer](members/sas_explorer) A tool to print metadata of a SAS 
    SAS7BDAT file
* [SSA to FIPS Mapping](members/ssa2fips) A utility to create in-database 
    mapping table between SSA and FIPS codes 
* [ZIP to FIPS mappings](members/zip2fips) A utility to download ZIP to 
    FIPS mapping from HUD site

```{seealso}
 [Python Module Index](modindex)
```
 