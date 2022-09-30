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

CREATE EXTENSION IF NOT EXISTS  hll;

CREATE OR REPLACE FUNCTION "public"."count_rows" (
    schema_name character varying, table_name character varying
)  RETURNS bigint
  VOLATILE
AS $body$
DECLARE cnt bigint;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, table_name) into cnt;
    RETURN cnt;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION "public"."estimate_rows" (
    schema_name character varying, table_name character varying
)  RETURNS bigint
  VOLATILE
AS $body$
DECLARE cnt bigint;
BEGIN
    SELECT reltuples::bigint
        FROM pg_class
        WHERE oid = (schema_name || '.' || table_name)::regclass
        INTO cnt;
    RETURN cnt;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION "public"."has_column" (
    s character varying,
    t character varying,
    c varchar
)  RETURNS VARCHAR
  VOLATILE
AS $body$
DECLARE e BOOL;
BEGIN
    SELECT EXISTS (
            SELECT * FROM information_schema.columns
            WHERE table_schema = s AND table_name = t AND column_name = c
    ) into e;
    RETURN e;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION "public"."get_year" (
    schema_name anyelement, table_name anyelement
)  RETURNS VARCHAR
  VOLATILE
AS $body$
DECLARE yr VARCHAR; s varchar; t varchar;
BEGIN
    s := schema_name::varchar;
    t := table_name::varchar;
    IF public.has_column(s, t, 'year') THEN
        EXECUTE format('SELECT string_agg(DISTINCT YEAR::INT::VARCHAR, '','') FROM %I.%I', s, t) into yr;
        RETURN yr;
    ELSIF public.has_column(s, t, 'observation_date') THEN
            EXECUTE format('SELECT string_agg(' ||
                           'DISTINCT EXTRACT (YEAR FROM observation_date)::INT::VARCHAR, '',''' ||
                           ') FROM %I.%I',
                schema_name, table_name) into yr;
            RETURN yr;
    ELSE
        RETURN NULL;
    END IF;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION public.zip_as_text(zip anyelement) RETURNS VARCHAR
IMMUTABLE
LANGUAGE plpgsql
AS $body$
BEGIN
    RETURN  btrim(to_char(zip::INT, '00000'));
END;
$body$ 
;


CREATE OR REPLACE PROCEDURE public.grant_select(
        username varchar
    )
LANGUAGE plpgsql
AS $body$
DECLARE
    sch text;
BEGIN
    FOR sch IN SELECT nspname FROM pg_namespace
    LOOP
        EXECUTE format($$ GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I $$, sch, username);
        EXECUTE format($$ GRANT USAGE ON SCHEMA %I TO %I $$, sch, username);
    END LOOP;
END;
$body$;
;

CREATE OR REPLACE FUNCTION public.hll_arr_agg(
    arr anyarray
) RETURNS HLL
IMMUTABLE
LANGUAGE plpgsql
AS $body$
DECLARE hash HLL;
BEGIN
    SELECT
        hll_add_agg(hx)
    FROM (
        SELECT
            hll_hash_any(x, 1) AS hx
        FROM (
            SELECT UNNEST(arr) AS x
        ) AS y
    ) AS hy into hash;
    RETURN hash;
END;
$body$
;
