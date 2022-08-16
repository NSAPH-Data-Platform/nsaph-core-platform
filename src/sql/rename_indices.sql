--  Copyright (c) 2022-2022. Harvard University
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
 An utility procedure aimed to rename indicies

 Parameters:
 schema_name | varchar | Target schema
 table_name | varchar | Target table
 what | varchar | An old index name
 how | varchar | A new index name
 */

CREATE OR REPLACE PROCEDURE public.rename_indices(
        schema_name varchar,
        table_name varchar,
        what varchar,
        how varchar
    )
LANGUAGE plpgsql
AS $$
DECLARE
    i_cursor CURSOR FOR
        SELECT
            schemaname,
            indexname
        FROM
            pg_indexes
        WHERE
            schemaname = schema_name and tablename = table_name
        ORDER BY
            tablename,
            indexname
    ;
    new_name varchar;
    cmd varchar;
BEGIN
    FOR idx in i_cursor LOOP
        new_name := replace(idx.indexname, what, how);
        cmd := format(
            'ALTER INDEX %I.%I RENAME TO %I',
            idx.schemaname, idx.indexname, new_name
        );
        EXECUTE cmd;
    END LOOP;
END;
$$;

