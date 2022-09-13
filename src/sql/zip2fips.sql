--  Copyright (c) 2022. Harvard University
--
--  Developed by Research Software Engineering,
--  Faculty of Arts and Sciences, Research Computing (FAS RC)
--  Author: Michael A Bouzinier
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--

/*
 Purpose:
 Responsible for in-database mapping between postal zip codes
 and state FIPS codes
 */

CREATE MATERIALIZED VIEW public.zip2fips AS
SELECT
    zip,
    YEAR,
    MONTH,
    string_agg(DISTINCT fips2s, ',') AS states,
    (   SELECT
            fips3s
        FROM
            public.hud_zip2fips AS y
        WHERE
            y.zip = x.zip
        AND y.year = x.year
        AND y.month = x.month
        ORDER BY
            res_ratio DESC
        LIMIT
            1) AS fips3,
    (   SELECT
            res_ratio
        FROM
            public.hud_zip2fips AS y
        WHERE
            y.zip = x.zip
        AND y.year = x.year
        AND y.month = x.month
        ORDER BY
            res_ratio DESC
        LIMIT
            1) AS ratio,
       COUNT(*) AS num_counties,
       (CASE COUNT(*) WHEN 1 THEN true ELSE false END) AS exact,
    (   SELECT
            string_agg(fips3s || ':' || res_ratio::TEXT, ';' ORDER BY res_ratio DESC)
        FROM
            public.hud_zip2fips AS y
        WHERE
            y.zip = x.zip
        AND y.year = x.year
        AND y.month = x.month) AS fipses
FROM
    public.hud_zip2fips AS x
GROUP BY
    YEAR,
    MONTH,
    zip
;

CREATE INDEX z2f_yz1_idx on public.zip2fips (year, zip) include (fips3);
CREATE INDEX z2f_z2_idx on public.zip2fips (zip);
CREATE INDEX z2f_r_idx on public.zip2fips (ratio);
CREATE INDEX z2f_c_idx on public.zip2fips (num_counties);
CREATE INDEX z2f_exact_idx on public.zip2fips ((1)) WHERE not exact;


CREATE OR REPLACE FUNCTION "public"."zip_to_fips" (
    _year int, _zip int
)  RETURNS int
  VOLATILE
AS $body$
DECLARE fips int;
BEGIN
    IF _year < 2010 THEN _year := 2010; END IF;
    SELECT fips3 into fips FROM public.zip2fips
        WHERE year = _year and zip = _zip
    LIMIT 1;
    IF fips is NULL AND _year < 2021 THEN
        RETURN public.zip_to_fips(_year + 1, _zip);
    END IF;
RETURN fips;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION "public"."zip_to_fips3" (
    _year int, _zip int
)  RETURNS varchar(3)
  VOLATILE
AS $body$
DECLARE fips int;
BEGIN
    fips := public.zip_to_fips(_year, _zip);
    RETURN btrim(to_char(fips, '000'));
END;
$body$ LANGUAGE plpgsql
;


CREATE OR REPLACE FUNCTION "public"."is_zip_to_fips_exact" (
    _year int, _zip int
)  RETURNS bool
  VOLATILE
AS $body$
DECLARE ex bool;
BEGIN
    IF _year < 2010 THEN RETURN false; END IF;
    SELECT exact into ex FROM public.zip2fips
        WHERE year = _year and zip = _zip
    LIMIT 1;
    IF ex is NULL THEN
        RETURN false;
    END IF;
RETURN ex;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION "public"."validate_zip_fips" (
    _zip int, fips2 varchar, fips3 varchar
)  RETURNS bool
  VOLATILE
AS $body$
DECLARE cnt int;
BEGIN
    SELECT 1 into cnt FROM
        public.hud_zip2fips
    WHERE
        zip = _zip AND fips3s = fips3 and fips2s = fips2
    LIMIT 1;
    RETURN cnt is not NULL;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION public.fips2state(
    state_fips VARCHAR
) RETURNS VARCHAR
IMMUTABLE
LANGUAGE plpgsql
AS $body$
DECLARE s VARCHAR;
BEGIN
    SELECT state_id FROM public.us_states WHERE fips2 = state_fips INTO s;
    RETURN s;
END;
$body$
;
CREATE OR REPLACE FUNCTION public.fips2state(
    state_fips INT
) RETURNS VARCHAR
IMMUTABLE
LANGUAGE plpgsql
AS $body$
DECLARE s VARCHAR;
BEGIN
    SELECT state_id FROM public.us_states WHERE fips2 = btrim(to_char(state_fips, '00')) INTO s;
    RETURN s;
END;
$body$
;
CREATE OR REPLACE FUNCTION public.fips2state_iso(
    state_fips VARCHAR
) RETURNS VARCHAR
IMMUTABLE
LANGUAGE plpgsql
AS $body$
DECLARE s VARCHAR;
BEGIN
    SELECT iso FROM public.us_states WHERE fips2 = state_fips INTO s;
    RETURN s;
END;
$body$
;
