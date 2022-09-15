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

-- Table maps SSA codes for US States and counties to FIPS codes

CREATE TABLE
    ssa
    (
        YEAR   INTEGER NOT NULL,
        state  CHARACTER VARYING(2),
        county CHARACTER VARYING(128),
        fips5  CHARACTER VARYING(5) NOT NULL,
        fips2  CHARACTER VARYING(2),
        fips3  CHARACTER VARYING(3),
        ssa5   CHARACTER VARYING(5) NOT NULL,
        ssa2   CHARACTER VARYING(2),
        ssa3   CHARACTER VARYING(3),
        PRIMARY KEY (YEAR, fips5, ssa5)
    );

CREATE INDEX ssa_county_idx ON public.ssa (county);
CREATE INDEX ssa_fips2_idx ON public.ssa (fips2);
CREATE INDEX ssa_fips3_idx ON public.ssa (fips3);
CREATE INDEX ssa_fips5_idx ON public.ssa (fips5);
CREATE INDEX ssa_ssa2_idx ON public.ssa (ssa2);
CREATE INDEX ssa_ssa3_idx ON public.ssa (ssa3);
CREATE INDEX ssa_ssa5_idx ON public.ssa (ssa5);
CREATE INDEX ssa_state_idx ON public.ssa (state);
CREATE INDEX ssa_year_idx ON public.ssa (year);
CREATE INDEX ssa_ssa23_idx ON public.ssa (year, ssa2, ssa3, fips2, fips3);

