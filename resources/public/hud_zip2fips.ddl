CREATE TABLE public.hud_zip2fips (
    year integer,
    month integer,
    zip integer,
    county integer,
    fips2i integer,
    fips3i integer,
    fips2s varchar(2),
    fips3s varchar(3),
    zip5s varchar(5),
    res_ratio float,
    bus_ratio float,
    oth_ratio float,
    tot_ratio float,
    usps_zip_pref_city varchar(128),
    usps_zip_pref_state varchar(2)
);

CREATE INDEX z2f_zip_idx ON public.hud_zip2fips USING btree (zip);
CREATE INDEX z2f_zip2_idx ON public.hud_zip2fips USING btree (zip5s);
CREATE INDEX z2f_cnty_idx ON public.hud_zip2fips USING btree (county);
CREATE INDEX z2f_f2i_idx ON public.hud_zip2fips USING btree (fips2i);
CREATE INDEX z2f_f2s_idx ON public.hud_zip2fips USING btree (fips2s);
CREATE INDEX z2f_f3i_idx ON public.hud_zip2fips USING btree (fips3i);
CREATE INDEX z2f_f3s_idx ON public.hud_zip2fips USING btree (fips3s);

CREATE INDEX z2f_zfs1_idx ON public.hud_zip2fips USING btree (zip, fips2i, fips3i);
CREATE INDEX z2f_zfs2_idx ON public.hud_zip2fips USING btree (zip, fips2s, fips3s);


CREATE INDEX z2f_yzip_idx ON public.hud_zip2fips USING btree (year, zip) INCLUDE (res_ratio, tot_ratio);
CREATE INDEX z2f_yzip2_idx ON public.hud_zip2fips USING btree (year, zip5s) INCLUDE (res_ratio, tot_ratio);
CREATE INDEX z2f_ycnty_idx ON public.hud_zip2fips USING btree (year, county) INCLUDE (res_ratio, tot_ratio);


