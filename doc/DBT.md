# Database Testing Framework

The testing framework allows to test a pipeline produces
exactly the same data in subsequent pipeline runs. The data is
tested in database table(s) procedure by the pipeline.


The framework consists of three utilities:

* [create_test.py](members/create_test.rst): A tool to generate a set of SQL queries
    testing that the data has not changed
* [dbt_runner.py](members/dbt_runner.rst): A tool to run the test cases generated
    by `create_test.py`
* [gen_dbt_cwl.py](members/gen_dbt_cwl): A tool to generate a CWL workflow
    that tests a given pipeline (also CWL workflow)
