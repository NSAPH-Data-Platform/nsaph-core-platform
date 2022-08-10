# How to query the database

```{contents}
---
local:
---
```

Here we show how to query the NSAPH database from Python.

We use public data (climate, pollution, census) in the query,
hence it can be executed in any environment.

## Setup

### Create Python Virtual Environment

First, we need to create a Python virtual environment. This can
be done with commands like:

```shell
python3 -m venv .nsaph
source .nsaph/bin/activate
```

### Install NSAPH packages

Next, we need to install NSAPH core packages. This can be done
using GitHub install:

```shell
python -m pip install git+https://github.com/NSAPH-Data-Platform/nsaph-utils.git
python -m pip install git+https://github.com/NSAPH-Data-Platform/nsaph-core-platform.git
```

## Create connection definition file

We need to create or update a database.ini file that stores connections
to the database. Here is a sample file I use:

```ini
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
```

> Note that the first connection uses my local instance of PostgreSQL
> on my laptop. The second connects to NSAPH database. It is using ssh
> tunnel to connect - this is defined by adding ssh_user parameter.
>
> **mbouzinier** is my username for both ssh and the database.


## Executing the query

We will use the following sample Python program to execute
a query (with public data) on the NSAPH database:
[query.py](members/sample_query)

Copy the file into your local directory and execute it:

```shell
python -u query.py database.ini nsaph2
```

## Using EXPLAIN to optimize queries

You do not want to run a query that will take a week to execute. When we have
hundreds of millions of records, this can easily happen. SQL is a declarative
language, hence, an SQL statement describes what you want to do. DBMS optimizer
decides how to do it. It should understand your query correctly. To ensure, it
did, use EXPLAIN query before trying to execute. See documentation for EXPLAIN.

Here are a few more links that might be useful:

* How to read [query plans](https://thoughtbot.com/blog/reading-an-explain-analyze-query-plan) produced by EXPLAIN
* [What is cost](https://scalegrid.io/blog/postgres-explain-cost/)

Unfortunately, less useful is the
[tutorial](https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-explain/)
The queries below (given as examples) take 4 to 8 minutes each. I suggest
running them with EXPLAIN first, note the costs and compare them with any costs
of the queries you will write. Pay attention how indices are used: the most
expensive part is scan.

