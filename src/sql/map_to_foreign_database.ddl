--  https://towardsdatascience.com/how-to-set-up-a-foreign-data-wrapper-in-postgresql-ebec152827f3
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER master FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '5432', dbname 'nsaph2');
CREATE USER MAPPING FOR nsaph SERVER master OPTIONS (user 'nsaph', password 'admin');
GRANT USAGE ON FOREIGN SERVER master TO nsaph;
CREATE SCHEMA epa;
IMPORT FOREIGN SCHEMA epa FROM SERVER master INTO epa;

