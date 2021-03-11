---
Building a Level 3 VM with PostgreSQL and Apache Superset
---

# Add DNS Records

Follow [FAS RC DNS Documentation](https://gitlab-int.rc.fas.harvard.edu/ops/dns)
to add  DNS data.

If you are installing Apache Airflow, Apache Superset or any other 
tool that uses Flask and gunicorn for Web Interface, you need to create 
virtual hosts for every service.

Therefore, add CNAME record for every virtual host below your A record. For example:

    lish                A   10.31.26.13    
    lish-superset       CNAME lish


# Create A VM in OpenNebula

Follow [this document](https://gitlab-int.rc.fas.harvard.edu/common/cloud-docs/-/blob/master/opennebula_new_vm_on_rc_puppet.md) 
to create a VM. Follow [Puppet Certificate Signing](https://docs-int.rc.fas.harvard.edu/puppet-certificate-signing-process/). 
To log in to puppetmaster, ssh there as root. This is important, only root user can see the certificate signing requests.

For vlan, use prod2448.

NSAPH VMs managed on https://cloud-int-holy.rc.fas.harvard.edu

# Create and attach a disk for data

## Create disk in OpenNebula

* Go to Storage -> Images tab
* Click green "+" icon
* Fill in the name
* Select:
  * Type: Generic storage datablock
  * Datastore (I used nese for my servers)
  * Image location: empty disk image. Selecting this option will make visible "Size" field
  * Fill in the required size
* Click green "Create" button    
* Go to the Instances -> VM Tab and attach the newly created disk    

## Attach file system to the VM
On the VM:

* Make sure the disk is physically attached by running `lsblk`
* Start `parted`
  * `(parted) mklabel gpt`
  * `(parted) print`  
  * `(parted) mkpart`
    * name
    * File system type? xfs
    * Start? 0% ## It is important to use "%"
    * End? 100%
  * Quit parted
* Run `fdisk -l` to ensure changes
* \[Optionally] Change file system type from 
  'Microsoft basic data' to 'Linux filesystem'
  * fdisk
    * `t`
    * `20`
    * `w`
* Create File system: `mkfs.xfs /dev/vdb1`
* Note UUID of the disk
  * `fdisk -l`
  * Or `ll /dev/disk/by-uuid/`  
  * Or `lsblk -f`
    
# Create and Sign SSL Certificate

Follow the [instructions](https://docs-int.rc.fas.harvard.edu/generate-csr-and-ssl-cert/)

Because we are creating virtual hosts we need to generate a multi-domain CSR

# Puppet Host File

You need to create a host file here:
https://gitlab-int.rc.fas.harvard.edu/puppet/puppet/-/tree/production/hieradata%2Fhosts

Include all Level 3 stuff as described 
[here](https://docs-int.rc.fas.harvard.edu/security-tooling-overview/#Auditbeat). 
In particular, remember Level 3 block, CrowdStrike and Auditbeat.

## Host File General
### Roles and Classes
    role: roles::service::docker::web
    mount_staff_nfs_vol: false

### Proxies

    yum_proxy: 'http://holyyumproxy:8123'

For HTTP proxies, use Environment:
    Environment="HTTP_PROXY=http://rcproxy.rc.fas.harvard.edu:3128"
    Environment="HTTPS_PROXY=http://rcproxy.rc.fas.harvard.edu:3128"

## Firewall

We need to add rules to:
 * Allow HTTPS (TCP 443) traffic at least from HPRC VPN 
    realm (10.255.12.0/26) (or wider)
 * Allow access to PostgreSQL (TCP 5432) from Docker containers (172.17.0.0/8)

## FAS RC Resources
### Files
#### Miscellaneous
 * Docker requires setting up HTTP proxies, 
   this is done in `/etc/systemd/system/docker.service.d/http-proxy.conf`
 * Nginx restart (I am personally unsure)
 * PostgreSQL custom configuration should be placed in
   `/etc/systemd/system/postgresql-13.service.d/override.conf` file. 
   We will be changing location of the database (essential for large 
   databases that can be much larger than system drive). Our solution 
   is to mount database drive on /data, hence we are setting `PGDATA`
   to `/data/pgsql/13/data/` To enforce some consistency, this is done 
   using local variable 
       `local::postgres::pgdata: '/data/pgsql/13/data/'`
   which is referenced where needed. [More Information](https://pgstef.github.io/2018/02/28/custom_pgdata_with_systemd.html) 
 * Actual directory /data/pgsql/13  

####Apache Superset

Puppet creates the following files required for Apache Superset: 

| File | Description
|------|------------
|docker-compose.yml | Docker-compose file, contains the majority of all settings
|superset/superset-config.py | Script that doecker executes within teh container
|superset/postresql/init/db-init.sql | database definitions
|startup.sh | script that can be used either to install and initialize Superset or as a list of commands that should be manually executed

Puppet does not initialize Superset, it should be done manually using the  
`startup.sh` script



### Exec

We use exec resource in Puppet to install additional software.

#### Installing docker-compose

Installation of docker-compose is described [here](https://docs.docker.com/compose/install/).

In host file this is done in `'/usr/local/bin/docker-compose'` section

#### Installing the latest version of PostgreSQL
The procedure is described in [PostgreSQL Documentation](https://www.postgresql.org/download/linux/redhat/)

We do it in `'Installs PostgreSQL 13'` section
### Setting system-wide HTTP proxy

We are setting system-wide environment variable to define HTTP proxy.
See `'Set HTTP Proxy'` section.


### Memory configuration for PostgreSQL

Most memory configuration parameters are defined in the file `postgresql.conf`
located in the PostgreSQL data directory. We override the default location of 
data directory, hence we are placing the new configuration in the location 
defined by `local::postgres::pgdata`. 

The necessary memory configuration is defined in the section: 
`local::postgres::conf`

Memory parameters are described in 
[PostgreSQL Resource Documentation](https://www.postgresql.org/docs/current/runtime-config-resource.html#RUNTIME-CONFIG-RESOURCE-MEMORY) 

If the database is used for data exploration, then `work_mem` is the most important 
parameter as it is responsible for resources allocated for aggregation and sorting 
and can make a difference of several orders of magnitude in time required
to execute an aggregate query.

Another important parameter is `maintenance_work_mem`, responsible for the speed of 
data ingestion and for operations such as building indices. 

Another set of parameters worth keeping in mind are those responsible for 
[Write Ahead Log](https://www.postgresql.org/docs/current/wal-configuration.html). 
They are most important for handling transactions and might be less so 
for Information Systems. More information can be found in 
[PostgreSQL Documentation](https://www.postgresql.org/docs/current/runtime-config-wal.html)

All the rest parameters are set to default values, therefore we can 
simply replace the original `postgresql.conf` with our custom one.

### Authentication Configuration for PostgreSQL

One parameter in `postgresql.conf` must be changed if any access, other than local 
via unix sockets is required: `listen_addresses`. This is almost always the case
as any third-party database tool or driver would need TCP/IP access besides
raw sockets. Though this parameter is set to "*" in the example, the actual control is 
defined by firewall rules and in the file `pg_hba.conf`. The original version 
of `pg_hba.conf` contains some important settings, therefore we add to the 
original copy rather than overriding it as we do with `postgresql.conf`. The 
content to be added is defined in `local::postgres::pg_hba`.

If Apache SUperset i sused, or any other tool that is installed as a 
docker container, then, at the very minimum, we shoudl allow access 
from docker networks (172.17.0.0/16 and 172.18.0.0/16 ). 

### Possible issue with PostgreSQL Client

PostgreSQL client is an executable called psql. Once PostgreSQL is installed,
one should be able to run:

`sudo -u postgres psql`

If the executable not found, then it means that the package `postgresql13`
is not installed. If you see an error:

`psql: symbol lookup error: psql: undefined symbol: PQsetErrorContextVisibility`

the most probable reason is that libraries from an older version of PostgreSQL
are installed. Run the following command:

`yum list installed | grep postg`

And if you see any packages without the latest version (13) - remove them and 
reinstall from latest version. 
For example:

```
[root@lish ~]# yum list installed | grep postg
postgresql-libs.x86_64              9.2.24-4.el7_8             @centos-7-base   
postgresql13.x86_64                 13.1-3PGDG.rhel7           @pgdg13          
postgresql13-libs.x86_64            13.1-3PGDG.rhel7           @pgdg13          
postgresql13-server.x86_64          13.1-3PGDG.rhel7           @pgdg13          
[root@lish ~]# yum autoremove postgresql-libs
...
[root@lish ~]# yum reinstall  postgresql13-libs
...
```

Note, that all libraries should be in `/usr/pgsql-13/lib/` directory, not
`/lib64`

Wrong:
```
[root@lish ~]# ldd /usr/pgsql-13/bin/psql 
	libpq.so.5 => /lib64/libpq.so.5 (0x00007fcc80494000)
```

Right:
```
[root@lish ~]# ldd /usr/pgsql-13/bin/psql 
	libpq.so.5 => /usr/pgsql-13/lib/libpq.so.5 (0x00007efeb84b4000)
```

## Nginx

### General Server Setup

 * Set Nginx to true
 * Define actual virtual hosts in `nginx::nginx_vhosts`

### SSL Setup 

[FAS RC SSL Documentation](https://docs-int.rc.fas.harvard.edu/generate-csr-and-ssl-cert/)
describes how to put the certificates. Put them for each of teh virtual hosts

 * Define certificates for virtual hosts in `profiles::web::ssl::certs`

### Setting up proxy servers for Flask Gunicorn Applications

Applications such as [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/) 
or [Apache Superset](https://superset.apache.org/) are using 
[Flask](https://flask.palletsprojects.com/en/1.1.x/) running on 
[Gunicorn](https://gunicorn.org/). This setup is described in a 
few documents on the net (e.g. https://medium.com/faun/deploy-flask-app-with-nginx-using-gunicorn-7fda4f50066a
or https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04)

We nned to configure nginx to be a proxy server for them. The configuration
is doen in `location` block. Common options are defined in 
`local::nginx::proxy: &proxy_pass` and specific proxy settings in 
`nginx::nginx_locations`
