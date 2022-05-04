CREATE TABLE public.us_iso (
    zip integer,
    lat numeric(9,5),
    lng numeric(11,5),
    city character varying,
    state_id character varying,
    state_name character varying,
    zcta character varying,
    parent_zcta character varying,
    population integer,
    density numeric(8,1),
    county_fips integer,
    county_name character varying,
    county_weights character varying,
    county_names_all character varying,
    county_fips_all character varying,
    imprecise character varying,
    military character varying,
    timezone character varying,
    iso character varying
);

CREATE INDEX us_iso_county_fips_idx ON public.us_iso USING btree (county_fips) INCLUDE (iso);
CREATE INDEX us_iso_county_name_idx ON public.us_iso USING btree (county_name);
CREATE INDEX us_iso_state_name_idx ON public.us_iso USING btree (state_name);
CREATE INDEX us_iso_zip_idx ON public.us_iso USING btree (zip);


