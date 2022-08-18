# Mapping between different territorial codes

```{contents}
---
local:
---
```

## Code systems

* United States Postal Service (USPS)
    * ZIP codes
* Federal Information Processing Standard (FIPS) codes
    * States
    * Counties
* Social Security Administration (SSA) Codes
    * States
    * Counties

## Mapping sources

Mapping between FIPS and SSA codes is available from
[National Bureau of Economic Research (NBER)](https://www.nber.org/research/data/ssa-federal-information-processing-series-fips-state-and-county-crosswalk)

Mapping between FIPS and ZIP codes we have taken from
United States Department of Housing and Urban Development (HUD)
Office of Policy Development and Research (PD&R)
[ZIP crosswalk files](https://www.huduser.gov/portal/datasets/usps_crosswalk.html)

## Python and SQL Code

Two utilities are provided to downloaded mapping files from
sources and load them into the database:

* Python module to download and ingest NBER FIPS <-> SSA mapping files:
    [ssa2fips](members/ssa2fips)
* Python module to download and ingest HUD ZIP to FIPS mappings:
    [zip2fips](members/zip2fips)
* DDL for HUD ZIP to FIPS mappings:
    [hud_zip2fips.ddl](members/hud_zip2fips)
* SQL to create direct ZIP => FIPS lookup table and helper functions:
    [zip2fips.sql](members/zip2fips.sql)

## Tables and Functions

All tables and functions are in schema "public".

### Tables

* `ssa`
* `hud_zip2fips`
* MATERIALIZED VIEW `public.zip2fips` for direct lookups

(territorial-codes-functions)=
### Functions 

* `public.zip_to_fips(year int, zip int) RETURNS int`
* `public.zip_to_fips(year int, zip int) RETURNS varchar` - the same as
    the above, just a different return type
* `public.is_zip_to_fips_exact(year int, zip int) RETURNS bool` returns
    `true` if county fips code can be unambiguously inferred from the given
    zip code in the given year, `false` otherwise. In particular this function
    return `false` if a `year` is missing from mapping files.
* `public.validate_zip_fips(zip int, fips2 varchar, fips3 varchar) RETURNS bool`
    returns true if a given combination of the codes is a valid combination,
    i.e. if there are places that in the given state and county that have the
    given zip code.
