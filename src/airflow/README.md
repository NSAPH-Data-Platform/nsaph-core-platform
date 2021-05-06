Airflow Configuration Files for NSAPH Data Server
=========================================

airflow.cfg must be placed in:

`/opt/projects/nsaph/home`

The location is specified in

`/etc/sysconfig/airflow`

A command to start airflow is:

`systemctl start airflow-webserver && systemctl start airflow-scheduler`