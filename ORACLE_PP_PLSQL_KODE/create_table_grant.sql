--drop table dvh_fam_pp.fam_pp_meta_data;
create table dvh_fam_pp.fam_pp_meta_data (
  pk_pp_meta_data number(38,0) not null,
  behandlings_id varchar2(255),
  kafka_mottatt_dato timestamp(3) not null,
  kafka_offset number(38,0),
  kafka_partition number(38,0),
  kafka_topic varchar2(255),
  lastet_dato timestamp(3),
  melding clob,
  primary key (pk_pp_meta_data)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_meta_data to dvh_fampp_kafka;

--drop table dvh_fam_pp.fam_pp_fagsak;
create table dvh_fam_pp.fam_pp_fagsak (
  pk_pp_fagsak number(38,0) not null,
  behandlings_id varchar2(255),
  forrige_behandlings_id varchar2(255),
  fk_person1_mottaker number(38,0),
  fk_person1_pleietrengende number(38,0),
  kafka_offset number(38,0),
  kafka_partition number(38,0),
  kafka_topic varchar2(255),
  lastet_dato timestamp(3),
  pleietrengende varchar2(255),
  relasjon varchar2(255),
  saksnummer varchar2(255),
  soker varchar2(255),
  utbetalingsreferanse varchar2(255),
  ytelse_type varchar2(255),
  vedtaks_tidspunkt timestamp(3),
  fk_pp_metadata number(38,0),
  primary key (pk_pp_fagsak),
  constraint fam_pp_fagsak_1 foreign key (fk_pp_metadata) references dvh_fam_pp.fam_pp_meta_data (pk_pp_meta_data)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_fagsak to dvh_fampp_kafka;


create table dvh_fam_pp.fam_pp_diagnose (
  pk_pp_diagnose number(38,0) not null,
  kode varchar2(6),
  type varchar2(10),
  fk_pp_fagsak number(38,0),
  primary key (pk_pp_diagnose),
  constraint fam_pp_diagnose_1 foreign key (fk_pp_fagsak) references dvh_fam_pp.fam_pp_fagsak(pk_pp_fagsak)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_diagnose to dvh_fampp_kafka;

--drop table dvh_fam_pp.fam_pp_perioder;
create table dvh_fam_pp.fam_pp_perioder (
  pk_pp_perioder number(38,0) not null,
  beredskap varchar2(100),
  brutto_beregningsgrunnlag number(38,0),
  dato_fom date,
  dato_tom date,
  gmt_andre_sokers_tilsyn varchar2(100),
  gmt_etablert_tilsyn number(5,2),
  gmt_overse_etablert_tilsyn_aarsak varchar2(100),
  gmt_tilgjengelig_for_soker number(5,2),
  nattevaak varchar2(100),
  oppgitt_tilsyn varchar2(10),
  pleiebehov number(5,2),
  sokers_tapte_timer varchar2(100),
  sokers_tapte_arbeidstid number(5,2),
  utfall varchar2(100),
  uttaksgrad number(5,2),
  fk_pp_fagsak number(38,0),
  primary key (pk_pp_perioder),
  constraint fam_pp_perioder_1 foreign key (fk_pp_fagsak) references dvh_fam_pp.fam_pp_fagsak (pk_pp_fagsak)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_perioder to dvh_fampp_kafka;


--drop table dvh_fam_pp.fam_pp_utbetalingsgrader;
create table dvh_fam_pp.fam_pp_periode_utbet_grader (
  pk_pp_periode_utbet_grader number(38,0) not null,
  arbeidsforhold_aktorid varchar2(20),
  arbeidsforhold_id varchar2(100),
  arbeidsforhold_orgnr varchar2(20),
  arbeidsforhold_type varchar2(100),
  dagsats number(38,0),
  delytelse_id_direkte varchar2(255),
  delytelse_id_refusjon varchar2(255),
  normal_arbeidstid varchar2(20),
  faktisk_arbeidstid varchar2(20),
  utbetalingsgrad number(5,2),
  bruker_er_mottaker varchar2(10),
  fk_pp_perioder number(38,0),
  primary key (pk_pp_periode_utbet_grader),
  constraint fam_pp_periode_utbet_grader_1 foreign key (fk_pp_perioder) references dvh_fam_pp.fam_pp_perioder (pk_pp_perioder)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_periode_utbet_grader to dvh_fampp_kafka;

create table dvh_fam_pp.fam_pp_periode_aarsak (
  pk_pp_periode_aarsak number(38,0) not null,
  aarsak varchar2(255),
  fk_pp_perioder number(38,0),
  primary key (pk_pp_periode_aarsak),
  constraint fam_pp_periode_aarsak_1 foreign key (fk_pp_perioder) references dvh_fam_pp.fam_pp_perioder (pk_pp_perioder)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_periode_aarsak to dvh_fampp_kafka;

create table dvh_fam_pp.fam_pp_periode_inngangsvilkaar (
  pk_pp_periode_inngangsvilkaar number(38,0) not null,
  utfall varchar2(20),
  vilkaar varchar2(20),
  fk_pp_perioder number(38,0),
  primary key (pk_pp_periode_inngangsvilkaar),
  constraint fam_pp_periode_inngangsvilkaar_1 foreign key (fk_pp_perioder) references dvh_fam_pp.fam_pp_perioder (pk_pp_perioder)
);
grant select,delete,update,insert on dvh_fam_pp.fam_pp_periode_inngangsvilkaar to dvh_fampp_kafka;

grant select on dt_person.dvh_person_ident_off_id to dvh_fampp_kafka;
create sequence dvh_fampp_kafka.hibernate_sequence increment by 1 start with 1;