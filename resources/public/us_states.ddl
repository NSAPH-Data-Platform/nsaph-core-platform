CREATE TABLE public.us_states (
  state_id character varying,
  state_name character varying,
  fips2 character varying,
  iso character varying,
  ssa2 character varying
);

CREATE INDEX states_st_idx on public.us_states (state_id);
CREATE INDEX states_stn_idx on public.us_states (state_name);
CREATE INDEX states_fips_idx on public.us_states (fips2);
CREATE INDEX states_iso_idx on public.us_states (iso);
CREATE INDEX states_ssa_idx on public.us_states (ssa2);
