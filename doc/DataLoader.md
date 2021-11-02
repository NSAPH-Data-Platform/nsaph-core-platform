# NSAPH Data Loader

## Usage

    python -u -m nsaph.loader.data_loader [-h] 
           [--data DATA [DATA ...]] [--pattern PATTERN [PATTERN ...]]
           [--reset] [--incremental] [--page PAGE] [--log LOG] [--limit LIMIT]
           [--buffer BUFFER] [--threads THREADS]
           [--parallelization {lines,files,none}] --domain DOMAIN
           [--registry REGISTRY] [--table TABLE] [--autocommit] [--db DB]
           [--connection CONNECTION]
    
    optional arguments:
      -h, --help            show this help message and exit
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
      --page PAGE           Explicit page size for the database, default: None
      --log LOG             Explicit interval for logging, default: None
      --limit LIMIT         Load at most specified number of records, default:
                            None
      --buffer BUFFER       Buffer size for converting fst files, default: None
      --threads THREADS     Number of threads writing into the database, default:
                            1
      --parallelization {lines,files,none}
                            Type of parallelization, if any, default: lines
      --domain DOMAIN       Name of the domain
      --registry REGISTRY   Path to domain registry. Registry is a directory or an
                            archive containing YAML files with domain definition.
                            Default is to use the built-in registry, default: None
      --table TABLE, -t TABLE
                            Name of the table to load data into, default: None
      --autocommit          Use autocommit, default: False
      --db DB               Path to a database connection parameters file,
                            default: database.ini
      --connection CONNECTION
                            Section in the database connection parameters file,
                            default: nsaph2