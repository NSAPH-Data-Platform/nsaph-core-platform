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

# Create and Sign SSL Certificate

Instructions are: https://docs-int.rc.fas.harvard.edu/generate-csr-and-ssl-cert/

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

## Exec

We use exec resource in Puppet to install additional software.

### Installing docker-compose

Installation of docker-compose is described [here](https://docs.docker.com/compose/install/).

In host file this is done in `'/usr/local/bin/docker-compose'` section

### Setting system-wide HTTP proxy

We are setting system-wide environment variable to define HTTP proxy.
See `'Set HTTP Proxy'` section.

### Installing the latest version of PostgreSQL
The procedure is described in [PostgreSQL Documentation](https://www.postgresql.org/download/linux/redhat/)

We do it in `'Installs PostgreSQL 13'` section


## Nginx

 * Set Nginx to true
 * Define certificates for virtual hosts in `profiles::web::ssl::certs`
 * Define actual virtual hosts in `nginx::nginx_vhosts`