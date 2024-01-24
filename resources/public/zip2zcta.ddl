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

-- Table maps USPS ZIP codes to ZCTA codes used by US Census

DROP TABLE IF EXISTS public.zip2zcta;
CREATE TABLE
    public.zip2zcta
    (
        YEAR   INTEGER NOT NULL,
        ZIP_CODE CHAR(5),
        ZIP_CODE_INT INT GENERATED ALWAYS AS (ZIP_CODE::INT) STORED,
        PO_NAME VARCHAR(64),
        STATE CHAR(2),
        ZIP_TYPE VARCHAR(64),
        ZCTA CHAR(12),
        ZCTA_INT INT GENERATED ALWAYS AS (
            --CASE WHEN ZCTA ~ '^[0-9]+$' THEN ZCTA::INT END
            CASE
                WHEN (btrim(ZCTA) ~ '^[0-9]+$')
                    THEN (zcta)::INTEGER
                    ELSE -1
            END
        ) STORED,
        zip_join_type VARCHAR(50),
        territory VARCHAR(10) GENERATED ALWAYS AS (
            CASE
                WHEN ("state" IN ('AK', 'PR', 'HI', 'VI', 'GU', 'MP', 'AS'))
                    THEN 'OFFSHORE'
                    ELSE '48'
            END
        ) STORED,
        zip_type_norm CHAR(1) GENERATED ALWAYS AS (
            CASE
                WHEN (lower(zip_type) IN ('p', 'post office or large volume customer')) THEN 'P'
                WHEN (lower(zip_type) IN ('s', 'u', 'zip code area')) THEN 'S'
                ELSE 'O'
            END
        ) STORED
        PRIMARY KEY (YEAR, ZIP_CODE, ZCTA)
    );

CREATE INDEX z_zip_idx ON public.zip2zcta (ZIP_CODE);
CREATE INDEX z_zcta_idx ON public.zip2zcta (ZCTA);
CREATE INDEX zi_zip_idx ON public.zip2zcta (ZIP_CODE_INT);
CREATE INDEX zi_zcta_idx ON public.zip2zcta (ZCTA_INT);
CREATE INDEX z_jt_idx ON public.zip2zcta (zip_join_type);
CREATE INDEX z_st_idx ON public.zip2zcta (STATE);
CREATE INDEX z_zt_idx ON public.zip2zcta (ZIP_TYPE);
CREATE INDEX z_y_zcta_idx ON public.zip2zcta (year, ZCTA);
CREATE INDEX zi_y_zcta_idx ON public.zip2zcta (year, ZCTA_INT);
CREATE INDEX zi_y_zip_idx ON public.zip2zcta (year, ZIP_CODE_INT);
CREATE INDEX z_zt2_idx ON public.zip2zcta (zip_type_norm);
CREATE INDEX z_t1_idx ON public.zip2zcta (territory);

CREATE OR REPLACE FUNCTION public.validate_zip2zcta() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $body$
    BEGIN
        IF EXISTS (
            SELECT FROM public.zip2zcta as t
            WHERE
              NEW.YEAR              = t.YEAR
              AND NEW.ZIP_CODE      = t.ZIP_CODE
              AND NEW.PO_NAME       = t.PO_NAME
              AND NEW.STATE         = t.STATE
              AND NEW.ZIP_TYPE      = t.ZIP_TYPE
              AND NEW.ZCTA          = t.ZCTA
              AND NEW.zip_join_type = t.zip_join_type
        ) THEN   -- Ignore duplicate records
            RETURN NULL;
        ELSIF NEW.ZCTA IS NULL THEN
            NEW.ZCTA := 'N/A';
        ELSIF NEW.ZCTA = 'No ZCTA' THEN
            NEW.ZCTA := 'N/A';
        ELSIF NEW.ZCTA = '' THEN
            NEW.ZCTA := 'N/A';
        END IF;
        RETURN NEW;
    END;

$body$;


CREATE TRIGGER zip2zcta_validation BEFORE INSERT ON public.zip2zcta
    FOR EACH ROW EXECUTE FUNCTION public.validate_zip2zcta();
