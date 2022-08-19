:orphan:

Database connection API
=======================

Use ini file to define database connections. The first block
in the example below connect directly to the database, while the
second block uses ssh tunnel to connect to database host.

.. code-block:: ini
   :caption: Example of database.ini file

    [mimic]
    host=localhost
    database=mimicii
    user=postgres
    password=*****

    [nsaph2]
    host=nsaph.cluster.uni.edu
    database=nsaph
    user=dbuser
    password=*********
    ssh_user=johndoe


.. automodule:: nsaph.db
   :members:
   :undoc-members:

