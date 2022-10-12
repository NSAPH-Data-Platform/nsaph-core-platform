# Project (Directory) Loading Utility

```{contents}
---
local:
---
```

(project-loader-overview)=
## Overview 

[Project Loader](members/project_loader)
is a command line tool to introspect and ingest into a database
a directory, containing CSV (or CSV-like, e.g. FST, JSON, SAS, etc.) files.
The directory can be structured, e.g. have nested subdirectories. All files
matching a certain name pattern at any nested subdirectory level
are included in the data set.

In the database, a schema is crated based on the given project name.
For each file in the data set a table is created. The name
of the table is constructed from the relative path of the
incoming data file with OS path separators (e.g. '/') being
replaced with underscores ('_').

It might be a good idea, before actually ingesting data into the database to
do a [dry run](#dry-runs-introspect-only) and visually examine the
database schema created by Introspection utility.

Loading into the database is performed using
[Data Loader](members/data_loader) functionality.

## Configuration options

Configuration options are provided by
[LoaderConfig](members/loader_config) object.
Usually, they are provided as command line arguments but can also be provided
via an API call.
                             
Some configuration options can be provided in the registry YAML
file. By default, if registry does not exist, a new YAML file 
will be created with the following parameters:

* header:  True ## i.e. CSV files are expected to have header line
* quoting: QUOTE_MINIMAL, ## i.e. only strings with whitespaces are
    expected to be quoted
* index:   "unless excluded"  ## We will build indices for every column
      unless it is explicitly excluded

See [Domain options](Datamodels.md#domain) for the descriptions
of these parameters.

When a registry file is created it can be manually edited by user. The
manual modifications will be preserved for subsequent runs.

## Usage from command line

```
    python -u -m nsaph.loader.project_loader
        [-h] [--drop]
        [--data DATA [DATA ...]]
        [--pattern PATTERN [PATTERN ...]]
        [--reset]
        [--incremental]
        [--sloppy]
        [--page PAGE]
        [--log LOG]
        [--limit LIMIT]
        [--buffer BUFFER]
        [--threads THREADS]
        [--parallelization {lines,files,none}]
        [--dryrun]
        [--autocommit]
        [--db DB]
        [--connection CONNECTION]
        [--verbose]
        [--table TABLE]
        --domain DOMAIN
        [--registry REGISTRY]

    optional arguments:
      -h, --help            show this help message and exit
      --drop                Drops domain schema, default: False
      --data DATA [DATA ...]
                            Path to a data file or directory. Can be a single CSV,
                            gzipped CSV or FST file or a directory recursively
                            containing CSV files. Can also be a tar, tar.gz (or
                            tgz) or zip archive containing CSV files, default:
                            None
      --pattern PATTERN [PATTERN ...]
                            pattern for files in a directory or an archive, e.g.
                            `**/maxdata_*_ps_*.csv`, default: None
      --reset               Force recreating table(s) if it/they already exist,
                            default: False
      --incremental         Commit every file and skip over files that have
                            already been ingested, default: False
      --sloppy              Do not update existing tables, default: False
      --page PAGE           Explicit page size for the database, default: None
      --log LOG             Explicit interval for logging, default: None
      --limit LIMIT         Load at most specified number of records, default:
                            None
      --buffer BUFFER       Buffer size for converting fst files, default: None
      --threads THREADS     Number of threads writing into the database, default:
                            1
      --parallelization {lines,files,none}
                            Type of parallelization, if any, default: lines
      --dryrun              Dry run: do not load any data, default: False
      --autocommit          Use autocommit, default: False
      --db DB               Path to a database connection parameters file,
                            default: database.ini
      --connection CONNECTION
                            Section in the database connection parameters file,
                            default: nsaph2
      --verbose             Verbose output, default: False
      --table TABLE, -t TABLE
                            Name of the table to manipulate, default: None
      --domain DOMAIN       Name of the domain
      --registry REGISTRY   Path to domain registry. Registry is a directory or an
                            archive containing YAML files with domain definition.
                            Default is to use the built-in registry, default: None
```
         
## Sample command

The following command creates a schema named `my_schema` and loads 
tables from all files with extension `.csv` found recursively under the 
directory `/data/incoming/valuable/data/`:

    python -u -m nsaph.loader.project_loader --domain my_schema --data /data/incoming/valuable/data/ --registry my_temp_schema.yaml --reset --pattern *.csv --db database.ini --connection postgres

It uses `database.ini` file in the current directory 
(where the program is started) and a section named `postgres` inside it. 
It creates temporary file 
`my_temp_schema.yaml` also in the current directory. If such a file 
already exists, it will be loaded and the settings found in it will override 
the defaults. Option `--reset` would delete all existing tables with 
the same names and recreate them.

The following is the same command but with parallel execution using 4 
threads writing into the database and with increased page size for writing 
into the database. It is optimized for hosts with more RAM.

    python -u -m nsaph.loader.project_loader --domain my_schema --data /data/incoming/valuable/data/ --reset --registry my_temp_schema.yaml --pattern *.csv --db database.ini --connection postgres --threads 4 --page 10000


## Dry runs (introspect only)

To just introspect files in a directory and generate YAML schema for
the project (see [domain schema specification](Datamodels) for
the description of the format) without modifications in the database,
use dry run. On the command line, just give :code:`--dryrun` option.

Dry run will create "registry" file that can be manually examined and 
modified. The following command described [above](#sample-command)
will perform dry run:

    python -u -m nsaph.loader.project_loader --domain my_schema --data /data/incoming/valuable/data/ --registry my_temp_schema.yaml --dryrun --pattern *.csv --db database.ini --connection postgres

This command will create file named `my_temp_schema.yaml`.    

## API Usage

Example of API usage retrieving command line arguments:

```python
    loader = ProjectLoader()
    loader.run()
```

More advanced usage:

```python
    config = LoaderConfig(__doc__).instantiate()
    config.pattern = "**/*.csv.gz"
    loader = ProjectLoader(config)
    loader.run()
```
