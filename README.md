# NSAPH Data Platform Core

[Package Documentation](https://nsaph-data-platform.github.io/nsaph-platform-docs/common/core-platform/)                                
[Documentation Home](https://nsaph-data-platform.github.io/nsaph-platform-docs/home.html)

## Overview
The data platform provides generic infrastructure for NSAPH Data Platform
It depends on nsaph_util package, but it augments it
with APIs and command line utilities dependent on the infrastructure 
and the environment. For instance, its components assume presence of PostgreSQL
DBMS (version 13 or later) and CWL runtime environment.

Some mapping (or crosswalk) tables are also included in the Core
Platform module. These tables include between different
territorial codes, such as USPS ZIP codes, Census ZCTA codes,
FIPS codes for US states
and counties, SSA codes for codes for US states
and counties. See more information in the
[Mapping between different territorial codes](doc/TerritorialCodes.md)

## Deployment

The best way to deploy the platform is by using 
[NSAPH Data Platform Deployment](https://nsaph-data-platform.github.io/nsaph-platform-docs/common/platform-deployment/doc/index.html)
package.

