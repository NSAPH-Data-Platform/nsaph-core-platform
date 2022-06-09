# Project (Directory) Loading Utility

<!-- toc -->

- [Overview](#overview)
- [Configuration options](#configuration-options)
- [Usage from command line](#usage-from-command-line)
- [Dry runs (introspect only)](#dry-runs-introspect-only)
- [API Usage](#api-usage)

<!-- tocstop -->

## Overview

[Project Loader](../src/python/nsaph/loader/project_loader.py) 
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
                                 
Loading into the database is performed using 
[Data Loader](DataLoader.md) functionality.

## Configuration options

Configuration options are provided by 
[LoaderConfig](../src/python/nsaph/loader/loader_config.py) object.
Usually, they are provided as command line arguments but can also be provided
via an API call.

## Usage from command line

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
                            "**/maxdata_*_ps_*.csv", default: None
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
   

## Dry runs (introspect only)

To just introspect files in a directory and generate YAML schema for
the project (see [domain schema specification](Datamodels.md) for 
the description of the format) without modifications in the database, 
use dry run. On the command line, just give `--dryrun` option.

## API Usage
Example of API usage retrieving command line arguments:

    loader = ProjectLoader()
    loader.run()
                           
More advanced usage:

    config = LoaderConfig(__doc__).instantiate()
    config.pattern = "**/*.csv.gz"
    loader = ProjectLoader(config)
    loader.run()

