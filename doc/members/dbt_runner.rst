Utility to generate test queries
================================

Usage
-----


 python -m nsaph.dbt.dbt_runner [-h] --script SCRIPT [SCRIPT ...]
        [--autocommit] [--db DB] [--connection CONNECTION]
        [--verbose] [--table TABLE]

Options:
  -h, --help            show this help message and exit
  --script SCRIPT [SCRIPT ...], -s SCRIPT [SCRIPT ...]
                        Path to the file(s) containing test scripts. When
                        generating test, the file is used to output the
                        script, while when running the tests, the scripts from
                        the specified files are executed
  --autocommit          Use autocommit, default: False
  --db DB, --database DB
                        Path to a database connection parameters file,
                        default: database.ini
  --connection CONNECTION, --connection_name CONNECTION
                        Section in the database connection parameters file,
                        default: nsaph2
  --verbose             Verbose output, default: False
  --table TABLE, -t TABLE
                        Name of the table to manipulate, default: None



Details
-------

.. automodule:: nsaph.dbt.dbt_runner
   :members:
   :undoc-members:

