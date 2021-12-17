/*
 * Copyright (c) 2021. Harvard University
 *
 * Developed by Research Software Engineering,
 * Faculty of Arts and Sciences, Research Computing (FAS RC)
 * Author: Michael A Bouzinier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

--  https://towardsdatascience.com/how-to-set-up-a-foreign-data-wrapper-in-postgresql-ebec152827f3
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER master FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '5432', dbname 'nsaph2');
CREATE USER MAPPING FOR nsaph SERVER master OPTIONS (user 'nsaph', password 'admin');
GRANT USAGE ON FOREIGN SERVER master TO nsaph;
CREATE SCHEMA epa;
IMPORT FOREIGN SCHEMA epa FROM SERVER master INTO epa;

