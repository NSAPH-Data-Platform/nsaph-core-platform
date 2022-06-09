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
CREATE OR REPLACE FUNCTION "public"."get_year" (
    schema_name character varying, table_name character varying
)  RETURNS VARCHAR
  VOLATILE
AS $body$
DECLARE yr VARCHAR;
BEGIN
EXECUTE format('SELECT string_agg(DISTINCT YEAR::INT::VARCHAR, '','') FROM %I.%I', schema_name, table_name) into yr;
RETURN yr;
END;
$body$ LANGUAGE plpgsql
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


