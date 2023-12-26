# Generator of pipelines executing containerized apps

```{contents}
---
local:
---
```

## Introduction

[National Studies on Air Pollution and Health](https://www.hsph.harvard.edu/nsaph/)
organization (NSAPH) publishes containerized applications to produce
certain types of data. These applications are published on the
[NSAPH Data Production GitHub](https://github.com/NSAPH-Data-Processing).

The Pipeline Generator generates a 
[CWL](https://www.commonwl.org/) pipeline to execute the app and ingest
the data it produces into Dorieh Data warehouse.

The process of data ingestion consists of two steps:

1. Generation of the piepline for data ingestion
2. Execution of the pipeline

             
## Prerequisites
        
### Docker or Python virtual environment

> You need either Option 1 or Option2, not both!

#### Option 1: Docker

The first step, generation of the pipeline, has minimal requirements. 
The easiest way to generate the pipeline is to use a docker container,
which will only requires Docker to be installed
on the host system, where the step is executed. See
[Docker installation instructions](https://docs.docker.com/engine/install/)
for details.
                     
#### Option 2: Python virtual environment

Alternatively, *instead* of Docker one can set up a
[Python virtual environment](https://docs.python.org/3/library/venv.html).
Once virtual environment is set up, you should install Dorieh packages
in it with the following command:

    pip install git+https://github.com/NSAPH-Data-Platform/nsaph-core-platform.git@develop


### Setup DBMS Server

Dorieh uses PostgreSQL DBMS to manage its data warehouse. The data warehouse
is assumed to be set up and operational to ingest data. Generating 
the pipeline does not require the data warehouse.

### Define connection
          
Dorieh uses database.ini type file to manage connections
to data warehouse. The format described in
[documentation](SampleQuery.md#create-connection-definition-file).

If the file with database connections does not exist, you 
need to create one. For example, named database.ini somewhere
on your local file system.

## Usage

### Generate pipeline and metadata

Generator takes 3 command line parameters:
                                       
1. GitHub URL or a local path for the containerized app. In the 
    root directory of the path, generator will look for a file named
   `app.config.yaml`.

If you use a local Python virtual environment, then run:

    python -m nsaph.apprunner.app_run_generator $GitHubURL $outputfile $branch

Example:

    python -m nsaph.apprunner.app_run_generator https://github.com/NSAPH-Data-Processing/zip2zcta_master_xwalk.git pipeline.cwl master

to generate a pipeline executing
[ZIP to ZCTA Crosswalk Producer app](https://github.com/NSAPH-Data-Processing/zip2zcta_master_xwalk)
using `master` branch and output the result into current directory in
a file named `pipeline.cwl`

Alternatively, to do the same using Docker container, execute:

    docker run -v $(pwd):/tmp/work forome/dorieh python -m nsaph.apprunner.app_run_generator https://github.com/NSAPH-Data-Processing/zip2zcta_master_xwalk.git /tmp/work/pipeline.cwl master

In both cases, the generator will produce 3 files:

* pipeline.cwl: main workflow file
* ingest.cwl: subworkflow used for data ingestion
* common.yaml: metadata required for ingestion. Name `common` is
    derived from `domain` key in the 
    [app.config.yaml](https://github.com/NSAPH-Data-Processing/zip2zcta_master_xwalk/blob/app-config-1/app.config.yaml)
    file in the app repository.
 
## Execute generated pipeline

If you installed Dorieh packages in your local Python virtual environment, 
you can execute the pipeline with the following command in 
the working directory, for example using CWL reference implementation
built into Dorieh (cwl-runner).

    cwl-runner pipeline.cwl --database $path_to_your connection_def_file --connection_name $connection_name

for example:

    cwl-runner pipeline.cwl --database ../../database.ini --connection_name dorieh
 
A better way would be to use a production grade CWL implementation
such as [Toil](https://toil.readthedocs.io/en/latest/running/cwl.html).
To do this you need to 
[install Toil](https://toil.readthedocs.io/en/latest/gettingStarted/install.html) 
on your local system. 

> You do not need to install Dorieh packages to execute the pipeline.
> The runtime engine will use Dorieh container where all requirements
> are preinstalled.

For Toil, a good advice would be to first create working 
directory, e.g. named `work`. Otherwise, Toil will create a default
directory somewhere in yoru temporary space. 

The command to execute the pipeline with Toil would be:

    toil-cwl-runner --retryCount 0 --cleanWorkDir never  --jobStore j1 --outdir results --workDir work  pipeline.cwl --database ../../database.ini --connection_name nsaph-docker

Specifying `jobStore` will let you restart the pipeline from a 
point of failure if pipeline execution fails for any reason.

## Appendix 1: Metadata description

### File app.config.yaml

Keys:

* `metadata`: a relative path to the app metadata file
* `dorieh-metadata`: a relative path to the metadata required to create
    a database table
* `docker`: information about the docker container, including:
  * `image`: the tag for the container image that executes the app
  * `run`: the command to be run within the container. This is an optional
    field
  * `outputdir`: directory, where to look for the results of the execution
    of the app

### File metadata.yml

This is the file referenced from  app.config.yaml by `metadata`
key.

It should contain the following keys:

* dataset_name
* description
* fields:
  * table
    * columns

Each column should have `name`, `type` and `description` keys.

### File dorieh-metadata.yaml

This is a header for a knowledge domain that will be created.
Detailed description is provided in the 
[Data modeling section](Datamodels.md#domain). It is important
to define correct values for `quoting`, `schema` and 
`primary_key` for each table.
