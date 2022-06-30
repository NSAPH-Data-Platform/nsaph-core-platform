# How to query the database

Here we show how to query the NSAPH database from Python.
                                                         
We use public data (climate, pollution, census) in the query,
hence it can be executed in any environment.

## Setup
        
### Create Python Virtual Environment
             
First, we need to create a Python virtual environment. This can
be done with commands like:

    python3 -m venv .nsaph
    source .nsaph/bin/activate
       
### Install NSAPH packages

Next, we need to install NSAPH core packages. This can be done 
using GitHub install:

    python -m pip install git+https://github.com/NSAPH-Data-Platform/nsaph-utils.git
    python -m pip install git+https://github.com/NSAPH-Data-Platform/nsaph-core-platform.git
    
## Create connection definition file

We need to create or update a database.ini file that stores connections
to the database. Here is a sample file I use:

    [mimic]
    host=localhost
    database=mimicii
    user=postgres
    password=*****
    
    [nsaph2]
    host=nsaph.rc.fas.harvard.edu
    database=nsaph2
    user=mbouzinier
    password=*********
    ssh_user=mbouzinier
    
> Note that the first connection uses my local instance of PostgreSQL
> on my laptop. The second connects to NSAPH database. It is using ssh 
> tunnel to connect - this is defined by adding ssh_user parameter.
> 
> **_mbouzinier_** is my username for both ssh and the database.

## Executing the query

We will use the following sample Python program to execute
a query (with public data) on the NSAPH database:
[query.py](../sandbox/python/examples/query.py)

Copy the file into your local directory and execute it:

    python -u query.py database.ini nsaph2



