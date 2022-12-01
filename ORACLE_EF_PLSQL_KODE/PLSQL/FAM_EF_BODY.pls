create or replace PACKAGE BODY                                                                                                                         FAM_EF AS
  procedure fam_ef_fk_person1(p_in_gyldig_dato in date, p_in_personident in varchar2, fk_person1 out varchar2) as
    --l_error_melding varchar2(1000);
  begin
    select max(fk_person1) keep
          (dense_rank first order by gyldig_fra_dato desc) as fk_person1
    into fk_person1
    from dt_person.dvh_person_ident_off_id_ikke_skjermet
    where off_id = p_in_personident
    and p_in_gyldig_dato between gyldig_fra_dato and gyldig_til_dato
    group by off_id;
  exception
    when others then
      fk_person1:= -1;
  end fam_ef_fk_person1;

   procedure fam_ef_utpakking_offset(p_in_offset in number, p_error_melding out varchar2) as
  v_pk_ef_fagsak number;
  v_pk_ef_utbetalinger number;
  v_pk_ef_vedtaksperioder number;
  v_pk_ef_vedtaksperioder_skole number;
  v_pk_ef_person number;--Barn
  v_pk_ef_vilkaar number;
  v_pk_ef_utgifter_skole number;
  v_pk_ef_delperiode_skole number;

  v_fk_person1_mottaker number;
  v_fk_person1_barn number;
  v_fk_person1_person number;--Barn

  v_lastet_dato date := sysdate;
  l_error_melding varchar2(4000);
  l_commit number := 0;
  l_feil_kilde_navn varchar2(100) := null;


  cursor cur_ef_fagsak(p_offset in number) is
    with jdata as (
      select fam_ef_meta_data.kafka_topic
            ,fam_ef_meta_data.kafka_offset
            ,fam_ef_meta_data.kafka_partition
            ,fam_ef_meta_data.pk_ef_meta_data
            ,fam_ef_meta_data.melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      left join dvh_fam_ef.fam_ef_fagsak
      on fam_ef_meta_data.kafka_offset = fam_ef_fagsak.kafka_offset
      where fam_ef_meta_data.kafka_offset = p_offset
      --and fam_ef_fagsak.kafka_offset is null
    )
    select t.fagsak_id, t.behandlings_id, t.person_ident, t.relatert_behandlings_id
          ,t.krav_mottatt, t.årsak_revurderings_kilde, t.revurderings_årsak
          ,t.adressebeskyttelse, t.vedtaksbegrunnelse_skole
          ,cast(to_timestamp_tz(t.vedtaks_tidspunkt,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as timestamp) as vedtaks_tidspunkt
          ,t.behandling_type, t.behandling_aarsak, t.vedtak_resultat
          ,cast(to_timestamp_tz(t.aktivitetsplikt_inntreffer_dato,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as aktivitetsplikt_inntreffer_dato
          ,t.har_sagt_opp_arbeidsforhold, t.stonadstype, t.funksjonell_Id
          ,kafka_topic, kafka_offset, kafka_partition, pk_ef_meta_data
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          fagsak_id                       varchar2 path '$.fagsakId'
         ,behandlings_id                  varchar2 path '$.behandlingId'
         ,person_ident                    varchar2 path '$.person.personIdent'
         ,relatert_behandlings_id         varchar2 path '$.relatertBehandlingId'
         ,adressebeskyttelse              varchar2 path '$.adressebeskyttelse'
         ,vedtaks_tidspunkt               varchar2 path '$.tidspunktVedtak'
         ,behandling_type                 varchar2 path '$.behandlingType'
         ,behandling_aarsak               varchar2 path '$.behandlingÅrsak'
         ,vedtak_resultat                 varchar2 path '$.vedtak'
         ,aktivitetsplikt_inntreffer_dato varchar2 path '$.aktivitetskrav.aktivitetspliktInntrefferDato'
         ,har_sagt_opp_arbeidsforhold     varchar2 path '$.aktivitetskrav.harSagtOppArbeidsforhold'
         ,stonadstype                     varchar2 path '$.stønadstype'
         ,funksjonell_Id                  varchar2 path '$.funksjonellId'
         ,vedtaksbegrunnelse_skole        varchar2 path '$.vedtaksbegrunnelse'
         ,krav_mottatt                    varchar2 path '$.kravMottatt'
         ,nested path '$.årsakRevurdering' columns (
         årsak_revurderings_kilde         varchar2 path '$.opplysningskilde'
         ,revurderings_årsak              varchar2 path '$.årsak'
         )
        )
        ) t;

  cursor cur_ef_utbetalinger(p_offset in number) is
    with jdata as (
      select kafka_topic
            ,kafka_offset
            ,kafka_partition
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
    )
    select t.belop, t.samordningsfradrag, t.inntekt, t.inntektsreduksjon
          ,cast(to_timestamp_tz(t.fra_og_med,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as fra_og_med
          ,cast(to_timestamp_tz(t.til_og_med,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as til_og_med
          ,t.person_ident, t.klassekode, t.delytelse_id
          ,kafka_topic, kafka_offset, kafka_partition
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path '$.utbetalinger[*]' columns (
         belop              varchar2 path '$.beløp'
        ,samordningsfradrag varchar2 path '$.samordningsfradrag'
        ,inntekt            varchar2 path '$.inntekt'
        ,inntektsreduksjon  varchar2 path '$.inntektsreduksjon'
        ,fra_og_med         varchar2 path '$.fraOgMed'
        ,til_og_med         varchar2 path '$.tilOgMed'
        ,person_ident       varchar2 path '$.utbetalingsdetalj.gjelderPerson.personIdent'
        ,klassekode         varchar2 path '$.utbetalingsdetalj.klassekode'
        ,delytelse_id       varchar2 path '$.utbetalingsdetalj.delytelseId'
         )
        )
        ) t;

  cursor cur_ef_vedtaksperioder(p_offset in number) is
    with jdata as (
      select kafka_topic
            ,kafka_offset
            ,kafka_partition
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
    )
    select cast(to_timestamp_tz(t.fra_og_med,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as fra_og_med
          ,cast(to_timestamp_tz(t.til_og_med,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as til_og_med
          ,t.aktivitet, t.periode_type
          ,kafka_topic, kafka_offset, kafka_partition
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path '$.vedtaksperioder[*]' columns (
         fra_og_med   varchar2 path '$.fraOgMed'
        ,til_og_med   varchar2 path '$.tilOgMed'
        ,aktivitet    varchar2 path '$.aktivitet'
        ,periode_type varchar2 path '$.periodeType'
         )
        )
        ) t;


        cursor cur_ef_tilleggstonader_KONTANTSTØTTE(p_offset in number) is
        with jdata as (
          select kafka_topic
                ,kafka_offset
                ,kafka_partition
                ,melding as doc
          from dvh_fam_ef.fam_ef_meta_data
          where kafka_offset = p_offset
        )
        select 'KONTANTSTØTTE' TYPE_TILLEGGS_STONAD
            ,to_date(t.fra_og_med,'yyyy-mm-dd')  as fra_og_med
                ,to_date(t.til_og_med,'yyyy-mm-dd')  as til_og_med
                    ,t.belop
              ,kafka_topic, kafka_offset, kafka_partition
        from jdata
            ,json_table
            (
             doc, '$'
             columns (
             nested path  '$.perioderKontantstøtte[*]' columns (
            FRA_OG_MED varchar2 path '$.fraOgMed'
            ,TIL_OG_MED varchar2 path '$.tilOgMed'
            ,BELOP number path '$.beløp'
             )
            )
            ) t;

    cursor cur_ef_tilleggstonader_TILLEGG(p_offset in number) is
        with jdata as (
          select kafka_topic
                ,kafka_offset
                ,kafka_partition
                ,melding as doc
          from dvh_fam_ef.fam_ef_meta_data
          where kafka_offset = p_offset
        )
        select 'TILLEGG' TYPE_TILLEGGS_STONAD
            ,to_date(t.fra_og_med,'yyyy-mm-dd')  as fra_og_med
                ,to_date(t.til_og_med,'yyyy-mm-dd')  as til_og_med
                    ,t.belop
              ,kafka_topic, kafka_offset, kafka_partition
        from jdata
            ,json_table
            (
             doc, '$'
             columns (
             nested path  '$.perioderTilleggsstønad[*]' columns (
            FRA_OG_MED varchar2 path '$.fraOgMed'
            ,TIL_OG_MED varchar2 path '$.tilOgMed'
            ,BELOP number path '$.beløp'
             )
            )
            ) t;

  cursor cur_ef_person(p_offset in number) is
    with jdata as (
      select kafka_topic
            ,kafka_offset
            ,kafka_partition
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
    )
    select t.person_ident
          ,cast(to_timestamp_tz(t.termindato,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as date) as termindato
          ,'BARN' as relasjon
          ,kafka_topic, kafka_offset, kafka_partition
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path  '$.barn[*]' columns (
         person_ident varchar2 path '$.personIdent'
        ,termindato   varchar2 path '$.termindato'
         )
        )
        ) t;

  cursor cur_ef_vilkaar(p_offset in number) is
    with jdata as (
      select kafka_topic
            ,kafka_offset
            ,kafka_partition
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
    )
    select t.*
          ,kafka_topic, kafka_offset, kafka_partition
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path  '$.vilkårsvurderinger[*]' columns (
         vilkaar      varchar2 path '$.vilkår'
        ,resultat     varchar2 path '$.resultat'
         )
        )
        ) t;


    -----------------Utpakking for Skolepeneger--------------------

    cursor cur_ef_vedtaksperioder_skole(p_offset in number) is
      with jdata as (
        select kafka_offset
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
      )
      select t.skoleaar
        , t.maks_sats_for_skoleaar
      from jdata
          ,json_table
          (
          doc, '$'
          columns (
          nested path '$.vedtaksperioder[*]' columns (
          skoleaar                  varchar2 path '$.skoleår'
          ,maks_sats_for_skoleaar    varchar2 path '$.maksSatsForSkoleår'
          )
          )
          ) t;


    cursor cur_ef_utgifter_skole(p_offset in number) is
      with jdata as (
        select kafka_offset
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
      )
      select to_date(t.utgiftsdato,'yyyy-mm-dd') as utgiftsdato
        ,t.utgiftsbelop
        , t.utbetaltbelop
      from jdata
          ,json_table
          (
          doc, '$'
          columns(
          nested path '$.vedtaksperioder[*]' columns (
          nested path '$.utgifter[*]' columns (
          utgiftsdato        varchar2 path '$.utgiftsdato'
         ,utgiftsbelop       varchar2 path '$.utgiftsbeløp'
         ,utbetaltbelop      varchar2 path '$.utbetaltBeløp'
          )
          )
          )) t;

    cursor cur_ef_delperiode_skole(p_offset in number) is
      with jdata as (
        select kafka_offset
            ,melding as doc
      from dvh_fam_ef.fam_ef_meta_data
      where kafka_offset = p_offset
      )
      select t.studie_type
        ,to_date(t.fra_og_med, 'yyyy-mm-dd') as fra_og_med
        ,to_date(t.til_og_med, 'yyyy-mm-dd') as til_og_med
        , t.studiebelastning
      from jdata
          ,json_table
          (
          doc, '$'
          columns(
          nested path '$.vedtaksperioder[*]' columns (
          nested path '$.perioder[*]' columns (
          studie_type           varchar2 path '$.studietype'
         ,fra_og_med            varchar2 path '$.datoFra'
         ,til_og_med            varchar2 path '$.datoTil'
         ,studiebelastning      varchar2 path '$.studiebelastning'
          )
          )
          )) t;


  begin
    for rec_fagsak in cur_ef_fagsak(p_in_offset) loop
      begin
        savepoint do_insert;
        begin
          v_pk_ef_fagsak := -1;
          v_fk_person1_mottaker := -1;
          select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_fagsak from dual;
          --Hent fk_person1
          fam_ef_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.person_ident, v_fk_person1_mottaker);

          insert into dvh_fam_ef.fam_ef_fagsak
          (
            pk_ef_fagsak, fk_ef_meta_data, fagsak_id, behandlings_id, relatert_behandlings_id
            ,krav_mottatt, årsak_revurderings_kilde, revurderings_årsak
           ,adressebeskyttelse
           ,fk_person1, behandling_type, behandlings_aarsak, vedtaks_status
           ,stonadstype, aktivitetsplikt_inntreffer_dato, har_sagt_opp_arbeidsforhold
           ,funksjonell_id, vedtaks_tidspunkt, kafka_topic, kafka_offset
           ,kafka_partition, lastet_dato, vedtaksbegrunnelse_skole


-- AKTIVITETSVILKAAR_BARNETILSYN


          )
          values
          (
            v_pk_ef_fagsak, rec_fagsak.pk_ef_meta_data, rec_fagsak.fagsak_id, rec_fagsak.behandlings_id
           ,rec_fagsak.relatert_behandlings_id, rec_fagsak.krav_mottatt, rec_fagsak.årsak_revurderings_kilde, rec_fagsak.revurderings_årsak
           ,rec_fagsak.adressebeskyttelse
           ,v_fk_person1_mottaker, rec_fagsak.behandling_type, rec_fagsak.behandling_aarsak
           ,rec_fagsak.vedtak_resultat, rec_fagsak.stonadstype, rec_fagsak.aktivitetsplikt_inntreffer_dato
           ,rec_fagsak.har_sagt_opp_arbeidsforhold, rec_fagsak.funksjonell_id, rec_fagsak.vedtaks_tidspunkt
           ,rec_fagsak.kafka_topic, rec_fagsak.kafka_offset, rec_fagsak.kafka_partition, v_lastet_dato, rec_fagsak.vedtaksbegrunnelse_skole
          );
        exception
          when others then
            l_error_melding := substr(sqlcode||sqlerrm,1,1000);
            l_feil_kilde_navn := 'FAM_EF_FAGSAK';
            p_error_melding := l_error_melding;
            rollback to do_insert; continue;
        end;

        --Utbetalinger
        for rec_utbetalinger in cur_ef_utbetalinger(rec_fagsak.kafka_offset) loop
          begin
            v_pk_ef_utbetalinger := -1;
            v_fk_person1_barn := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_utbetalinger from dual;

            --Hent fk_person1
            fam_ef_fk_person1(rec_utbetalinger.til_og_med, rec_utbetalinger.person_ident, v_fk_person1_barn);

            insert into dvh_fam_ef.fam_ef_utbetalinger
            (
              pk_ef_utbetalinger, fk_ef_fagsak, belop, samordningsfradrag, inntekt,
              inntektsreduksjon, fra_og_med, til_og_med, fk_person1, klassekode, delytelse_id,
              behandlings_id, kafka_topic, kafka_offset, kafka_partition, lastet_dato
            )
            values
            (
              v_pk_ef_utbetalinger, v_pk_ef_fagsak, rec_utbetalinger.belop,
              rec_utbetalinger.samordningsfradrag, rec_utbetalinger.inntekt,
              rec_utbetalinger.inntektsreduksjon, rec_utbetalinger.fra_og_med,
              rec_utbetalinger.til_og_med, v_fk_person1_barn, rec_utbetalinger.klassekode,
              rec_utbetalinger.delytelse_id,
              rec_fagsak.behandlings_id,
              rec_utbetalinger.kafka_topic, rec_utbetalinger.kafka_offset,
              rec_utbetalinger.kafka_partition, v_lastet_dato
            );
          exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_UTBETALINGER';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Utbetalinger

        --Vedtaksperioder
        for rec_vedtaksperioder in cur_ef_vedtaksperioder(rec_fagsak.kafka_offset) loop
          begin
            v_pk_ef_vedtaksperioder := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_vedtaksperioder from dual;

            insert into dvh_fam_ef.fam_ef_vedtaksperioder
            (
              pk_ef_vedtaksperioder, fk_ef_fagsak, fra_og_med, til_og_med
             ,aktivitet, periode_type
             ,behandlings_id, kafka_topic, kafka_offset, kafka_partition, lastet_dato
            )
            values
            (
              v_pk_ef_vedtaksperioder, v_pk_ef_fagsak, rec_vedtaksperioder.fra_og_med
             ,rec_vedtaksperioder.til_og_med, rec_vedtaksperioder.aktivitet
             ,rec_vedtaksperioder.periode_type
             ,rec_fagsak.behandlings_id
             ,rec_vedtaksperioder.kafka_topic, rec_vedtaksperioder.kafka_offset
             ,rec_vedtaksperioder.kafka_partition, v_lastet_dato
            );
          exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_VEDTAKSPERIODER';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Vedtaksperioder


        --Vedtaksperioder_skole
        for rec_vedtaksperioder_skole in cur_ef_vedtaksperioder_skole(rec_fagsak.kafka_offset) loop
          begin
            v_pk_ef_vedtaksperioder_skole := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_vedtaksperioder_skole from dual;
            insert into dvh_fam_ef.fam_ef_vedtaksperioder_skole
            (
              pk_ef_vedtaksperioder_skole, skoleaar, maks_sats_for_skoleaar, lastet_dato, fk_ef_fagsak
            )
            values
            (
              v_pk_ef_vedtaksperioder_skole, rec_vedtaksperioder_skole.skoleaar, rec_vedtaksperioder_skole.maks_sats_for_skoleaar
              , v_lastet_dato, v_pk_ef_fagsak
            );
         exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_VEDTAKSPERIODER_SKOLE';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Vedtaksperioder_skole

        --Person
        for rec_person in cur_ef_person(rec_fagsak.kafka_offset) loop
          begin
            v_pk_ef_person := -1;
            v_fk_person1_person := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_person from dual;
            --Hent fk_person1
            fam_ef_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_person.person_ident, v_fk_person1_person);

            insert into dvh_fam_ef.fam_ef_person
            (
              pk_ef_person
             ,fk_ef_fagsak
             ,fk_person1, termindato, relasjon, behandlings_id, kafka_topic, kafka_offset
             ,kafka_partition, lastet_dato
            )
            values
            (
              v_pk_ef_person
             ,v_pk_ef_fagsak
             ,v_fk_person1_person
             ,rec_person.termindato, rec_person.relasjon
             ,rec_fagsak.behandlings_id
             ,rec_person.kafka_topic, rec_person.kafka_offset
             ,rec_person.kafka_partition, v_lastet_dato
            );
          exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_PERSON';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Person

        --Vilkaar
        for rec_vilkaar in cur_ef_vilkaar(rec_fagsak.kafka_offset) loop
          begin
            v_pk_ef_vilkaar := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_vilkaar from dual;

            insert into dvh_fam_ef.fam_ef_vilkaar
            (
              pk_ef_vilkår, fk_ef_fagsak
             ,vilkaar, resultat, behandlings_id, kafka_topic, kafka_offset
             ,kafka_partition, lastet_dato
            )
            values
            (
              v_pk_ef_vilkaar
             ,v_pk_ef_fagsak
             ,rec_vilkaar.vilkaar, rec_vilkaar.resultat
             ,rec_fagsak.behandlings_id
             ,rec_vilkaar.kafka_topic, rec_vilkaar.kafka_offset
             ,rec_vilkaar.kafka_partition, v_lastet_dato
            );
          exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_VILKAAR';
              p_error_melding := substr(l_error_melding||' '||l_feil_kilde_navn, 1, 1000);
              rollback to do_insert; continue;
          end;
        end loop;--Vilkaar

        -------------------- Skolepenger---------------------


        --Utgifter_skole
        for rec_ugifter_skole in cur_ef_utgifter_skole(rec_fagsak.kafka_offset) loop
         begin
            v_pk_ef_utgifter_skole := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_utgifter_skole from dual;
            insert into dvh_fam_ef.fam_ef_utgifter_skole
            (
                pk_ef_utgifter_skole
                , utgiftsdato
                , utgiftsbelop, utbetaltbelop, lastet_dato, fk_ef_vedtaksperioder_skole
            )
            values
            (
               v_pk_ef_utgifter_skole
               , rec_ugifter_skole.utgiftsdato
               , rec_ugifter_skole.utgiftsbelop, rec_ugifter_skole.utbetaltbelop
               , v_lastet_dato, v_pk_ef_vedtaksperioder_skole
            );
            exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_UTGIFTER_SKOLE';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Utgifter_skole

        --Delperiode_skole
        for rec_delperiode_skole in cur_ef_delperiode_skole(rec_fagsak.kafka_offset) loop
         begin
            v_pk_ef_delperiode_skole := -1;
            select dvh_famef_kafka.hibernate_sequence.nextval into v_pk_ef_delperiode_skole from dual;
            insert into dvh_fam_ef.fam_ef_delperiode_skole
            (
                pk_ef_delperiode_skole, studie_type, fra_og_med, til_og_med
                , studiebelastning
                , lastet_dato
                , fk_ef_vedtaksperioder_skole
            )
            values
            (
               v_pk_ef_delperiode_skole, rec_delperiode_skole.studie_type, rec_delperiode_skole.fra_og_med, rec_delperiode_skole.til_og_med
               ,
               rec_delperiode_skole.studiebelastning, v_lastet_dato, v_pk_ef_vedtaksperioder_skole
            );
            exception
            when others then
              l_error_melding := substr(sqlcode||sqlerrm,1,1000);
              l_feil_kilde_navn := 'FAM_EF_DELPERIODE_SKOLE';
              p_error_melding := l_error_melding;
              rollback to do_insert; continue;
          end;
        end loop;--Delperiode_skole



        l_commit := l_commit + 1;
        if l_commit >= 10000 then
          commit;
          l_commit := 0;
          l_error_melding := null;
        end if;
      exception
        when others then
          l_error_melding := substr(sqlcode||sqlerrm,1,1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, null, l_error_melding, sysdate, 'FAM_EF_UTPAKKING1');
          p_error_melding := substr(p_error_melding || l_error_melding || 'FAM_EF_UTPAKKING1', 1, 1000);
          l_error_melding := null;
          --Gå videre til neste rekord
      end;
    end loop;--Fagsak
    commit;
    if l_error_melding is not null then
      insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
      values(null, l_error_melding, sysdate, l_feil_kilde_navn);
      commit;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end if;
  exception
    when others then
      l_error_melding := sqlcode || ' ' || sqlerrm;
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_EF_UTPAKKING2');
      commit;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  end fam_ef_utpakking_offset;



  procedure fam_ef_slett_offset(p_in_offset in varchar2, p_error_melding out varchar2) as
    v_temp_dml varchar2(4000);
    l_error_melding varchar2(1000);
  begin
    v_temp_dml := 'create global temporary table temp_tbl_slett
                   on commit preserve rows
                   as
                   select distinct fag.pk_ef_fagsak,
                          utb.pk_ef_utbetalinger,
                          utif.pk_ef_utgifter_skole,
                          del.pk_ef_delperiode_skole,
                          pers_utb.pk_ef_person,
                          vedtaksperioder.pk_ef_vedtaksperioder,
                          vedtaksperioder_skole.pk_ef_vedtaksperioder_skole,
                          vilkaar.pk_ef_vilkår

                   from dvh_fam_ef.fam_ef_meta_data meta

                   join dvh_fam_ef.fam_ef_fagsak fag
                   on meta.pk_ef_meta_data = fag.fk_ef_meta_data

                   left join dvh_fam_ef.fam_ef_utbetalinger utb
                   on fag.pk_ef_fagsak = utb.fk_ef_fagsak

                   left join dvh_fam_ef.fam_ef_vedtaksperioder_skole vedtaksperioder_skole
                   on fag.pk_ef_fagsak = vedtaksperioder_skole.fk_ef_fagsak

                   left join dvh_fam_ef.fam_ef_utgifter_skole utif
                   on utif.fk_ef_vedtaksperioder_skole = vedtaksperioder_skole.pk_ef_vedtaksperioder_skole

                   left join dvh_fam_ef.fam_ef_delperiode_skole del
                   on del.fk_ef_vedtaksperioder_skole = vedtaksperioder_skole.pk_ef_vedtaksperioder_skole

                   left join dvh_fam_ef.fam_ef_person pers_utb
                   on fag.pk_ef_fagsak = pers_utb.fk_ef_fagsak

                   left join dvh_fam_ef.fam_ef_vedtaksperioder vedtaksperioder
                   on fag.pk_ef_fagsak = vedtaksperioder.fk_ef_fagsak

                   join dvh_fam_ef.fam_ef_vilkaar vilkaar
                   on fag.pk_ef_fagsak = vilkaar.fk_ef_fagsak

                   where meta.kafka_offset = ' || p_in_offset;
    --dbms_output.put_line(v_temp_dml);
    execute immediate v_temp_dml;

    begin
      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_vilkaar
      WHERE pk_ef_vilkår IN (SELECT DISTINCT pk_ef_vilkår FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_delperiode_skole
      WHERE pk_ef_delperiode_skole IN (SELECT DISTINCT pk_ef_delperiode_skole FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_utgifter_skole
      WHERE pk_ef_utgifter_skole IN (SELECT DISTINCT pk_ef_utgifter_skole FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_vedtaksperioder_skole
      WHERE pk_ef_vedtaksperioder_skole IN (SELECT DISTINCT pk_ef_vedtaksperioder_skole FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_vedtaksperioder
      WHERE pk_ef_vedtaksperioder IN (SELECT DISTINCT pk_ef_vedtaksperioder FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_utbetalinger
      WHERE pk_ef_utbetalinger IN (SELECT DISTINCT pk_ef_utbetalinger FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_person
      WHERE pk_ef_person IN (SELECT DISTINCT pk_ef_person FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_ef.fam_ef_fagsak
      WHERE pk_ef_fagsak IN (SELECT DISTINCT pk_ef_fagsak FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      execute immediate v_temp_dml;

      commit;--Commit på alle
    exception
      when others then
        l_error_melding := sqlcode || ' ' || sqlerrm;
        rollback;
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, sysdate, 'FAM_EF_SLETT_OFFSET1');
        commit;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end;

    --ROLLBACK;--Test
    v_temp_dml := 'TRUNCATE TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    execute immediate v_temp_dml;

    v_temp_dml := 'DROP TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    execute immediate v_temp_dml;
    --COMMIT;
  exception
    when others then
      l_error_melding := sqlcode || ' ' || sqlerrm;
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_EF_SLETT_OFFSET2');
      commit;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  end fam_ef_slett_offset;

  procedure fam_ef_stonad_insert(p_in_period in varchar2
                                ,p_in_gyldig_flagg in number default 0
                                ,p_out_error out varchar2) as
    l_error_melding varchar2(1000);
    l_commit number := 0;
    l_dato_fom date;
    l_dato_tom date;

    cursor cur_ef(p_dato_fom in date, p_dato_tom in date) is
      with ur as
	  (
        select ur.*
        from dvh_fam_ef.fam_ef_ur_utbetaling ur
        where trunc(posteringsdato,'dd') between p_dato_fom and p_dato_tom
        and length(delytelse_id) > 1
        and length(henvisning) > 1
      ),
      ur1 as
      (
        select fk_person1, henvisning, fagsystem_id, dato_utbet_fom, dato_utbet_tom
        from ur
        group by fk_person1, henvisning, fagsystem_id, dato_utbet_fom, dato_utbet_tom
        having sum(belop_sign) != 0
      ),
      fam_ef_ur as
      (
        select ur.fk_person1
			  ,ur.HOVEDKONTONR
              ,sum(ur.belop_sign) belop_totalt
              ,to_number(to_char(ur.posteringsdato, 'yyyymm')) as periode
              ,max(ur.posteringsdato) as max_posteringsdato
              ,min(ur.dato_utbet_fom) as min_dato_utbet_fom
              ,max(ur.dato_utbet_tom) as max_dato_utbet_tom
              ,max(ur.henvisning) as max_henvisning
              ,max(ur.delytelse_id) as max_delytelse_id
              ,sum(case when ur.posteringsdato between ur.dato_utbet_fom and ur.dato_utbet_tom then ur.belop_sign
                        else 0
                   end) belop
              --,max(case when tid.dato between tid_fom.dato and tid_tom.dato then henvisning
                --   end) henvisning
              --,max(case when tid.dato between tid_fom.dato and tid_tom.dato then delytelse_id
                --   end) delytelse_id
              ,count(case when ur.posteringsdato between ur.dato_utbet_fom and ur.dato_utbet_tom then ur.delytelse_id
                     end) ant_delytelse_id
        from ur
        join ur1
        on ur.fk_person1 = ur1.fk_person1
        and ur.henvisning = ur1.henvisning
        and ur.fagsystem_id = ur1.fagsystem_id
        and ur.dato_utbet_fom = ur1.dato_utbet_fom
        and ur.dato_utbet_tom = ur1.dato_utbet_tom
        group by ur.fk_person1, ur.HOVEDKONTONR,to_number(to_char(ur.posteringsdato, 'yyyymm'))
      ),
      fagsak as
      (
        select fam_ef_ur.*, fagsak.fagsak_id
              ,fagsak.pk_ef_fagsak, fagsak.behandling_type, fagsak.stonadstype, fagsak.aktivitetsvilkaar_barnetilsyn aktivitetskrav
              ,fagsak.vedtaks_tidspunkt, fam_ef_utbetalinger.inntektsreduksjon inntfradrag
              ,fam_ef_utbetalinger.samordningsfradrag
              ,fam_ef_utbetalinger.inntekt innt
              ,dim_person_mottaker.pk_dim_person
              ,to_char(dim_person_mottaker.fodt_dato,'YYYY') fodsel_aar
              ,to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd
              ,dim_kjonn.kjonn_kode kjonn
              ,floor(months_between(fam_ef_ur.max_posteringsdato, dim_person_mottaker.fodt_dato)/12) alder
              ,case when vedtaks_perioder.periode_type = 'PERIODE_FØR_FØDSEL' then 1
                    when fagsak.stonadstype = 'OVERGANGSSTØNAD' then aktivitet_type.overgkode
               end overgkode
              --,case when fagsak.stonadstype = 'OVERGANGSSTØNAD' then fam_ef_ur.belop
              -- end ovgst
              --case when fagsak.stonadstype = 'BARNETILSYN' then fam_ef_ur.belop
              ,case when fam_ef_ur.hovedkontonr = '306' then fam_ef_ur.belop
               end ovgst,
              case when fam_ef_ur.hovedkontonr = '540' then fam_ef_ur.belop
               end barntil,
               case when fam_ef_ur.hovedkontonr = '542' then fam_ef_ur.belop
               end skolepen
             ,case when fam_ef_ur.hovedkontonr = '306' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_ovgst
             ,case when fam_ef_ur.hovedkontonr = '540' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_barntil
             ,case when fam_ef_ur.hovedkontonr = '542' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_skolepen
              ,(select min(utbet_forst.fra_og_med)
                from dvh_fam_ef.fam_ef_utbetalinger utbet_forst
                where utbet_forst.behandlings_id = fagsak.behandlings_id
               ) virk
              ,fam_ef_ur.belop + fam_ef_utbetalinger.inntektsreduksjon bruttobelop
              ,dim_geografi.kommune_nr, dim_geografi.bydel_kommune_nr
              ,dim_person_mottaker.statsborgerskap statsb, dim_person_mottaker.fodeland
              ,dim_person_mottaker.fk_dim_geografi_bosted
              ,sivilstatus.sivilstatus_kode sivst
              ,vedtaks_perioder.periode_type
              ,vedtaks_perioder.aktivitet
			  ,case when fam_ef_ur.hovedkontonr = '540' then vedtaks_perioder.antallbarn
               end Antbtg,
               case when fam_ef_ur.hovedkontonr = '540' then vedtaks_perioder.utgifter
               end Btdok
              ,aktivitet_type.aktkode
	          ,tilleggsstonader.belop belop_tillegg
              ,kontantstøtte.belop belop_kontantstøtte
              ,case when fam_ef_ur.hovedkontonr = '306' then fam_ef_ur.belop_totalt
               end ovgst_totalt
              ,case when fam_ef_ur.hovedkontonr = '540' then fam_ef_ur.belop_totalt
               end barntil_totalt
              ,case when fam_ef_ur.hovedkontonr = '542' then fam_ef_ur.belop_totalt
               end skolepen_totalt
              --,sysdate
        from fam_ef_ur
        left join dvh_fam_ef.fam_ef_fagsak fagsak
        on fam_ef_ur.max_henvisning = fagsak.funksjonell_id

        left outer join dt_person.dim_person dim_person_mottaker
        on dim_person_mottaker.fk_person1 = fagsak.fk_person1
        /*and dim_person_mottaker.gyldig_fra_dato <= fagsak.vedtaks_tidspunkt
        and dim_person_mottaker.gyldig_til_dato >= fagsak.vedtaks_tidspunkt*/
        and dim_person_mottaker.gyldig_fra_dato <= fam_ef_ur.max_posteringsdato
        and dim_person_mottaker.gyldig_til_dato >= fam_ef_ur.max_posteringsdato

        left outer join dt_kodeverk.dim_sivilstatus sivilstatus
        on sivilstatus.pk_dim_sivilstatus = dim_person_mottaker.fk_dim_sivilstatus

        left outer join dt_kodeverk.dim_kjonn
        on dim_kjonn.pk_dim_kjonn = dim_person_mottaker.fk_dim_kjonn

        left outer join dt_kodeverk.dim_geografi
        on dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi

        left join dvh_fam_ef.fam_ef_utbetalinger
        on fam_ef_utbetalinger.fk_ef_fagsak = fagsak.pk_ef_fagsak
        --and fam_ef_ur.min_dato_utbet_fom between fam_ef_utbetalinger.fra_og_med and fam_ef_utbetalinger.til_og_med
        and fam_ef_ur.max_dato_utbet_tom between fam_ef_utbetalinger.fra_og_med and fam_ef_utbetalinger.til_og_med

        left join dvh_fam_ef.fam_ef_vedtaksperioder vedtaks_perioder
        on vedtaks_perioder.fk_ef_fagsak = fagsak.pk_ef_fagsak
        --and fam_ef_ur.min_dato_utbet_fom between vedtaks_perioder.fra_og_med and vedtaks_perioder.til_og_med
        and fam_ef_ur.max_dato_utbet_tom between vedtaks_perioder.fra_og_med and vedtaks_perioder.til_og_med

        left join dvh_fam_ef.fam_ef_aktivitet_type aktivitet_type
        on vedtaks_perioder.aktivitet = aktivitet_type.aktivitet

		left join dvh_fam_ef.fam_ef_tilleggsstonader tilleggsstonader
        on tilleggsstonader.fk_ef_fagsak = fagsak.pk_ef_fagsak
        --and fam_ef_ur.min_dato_utbet_fom between tilleggsstonader.fra_og_med and tilleggsstonader.til_og_med
        and fam_ef_ur.max_dato_utbet_tom between tilleggsstonader.fra_og_med and tilleggsstonader.til_og_med
        and tilleggsstonader.TYPE_TILLEGGS_STONAD = 'TILLEGG'


        left join dvh_fam_ef.fam_ef_tilleggsstonader kontantstøtte
        on kontantstøtte.fk_ef_fagsak = fagsak.pk_ef_fagsak
        --and fam_ef_ur.min_dato_utbet_fom between kontantstøtte.fra_og_med and kontantstøtte.til_og_med
        and fam_ef_ur.max_dato_utbet_tom between kontantstøtte.fra_og_med and kontantstøtte.til_og_med
        and kontantstøtte.TYPE_TILLEGGS_STONAD = 'KONTANTSTØTTE'

      ),

      barn as
      (
        select fagsak.periode, fagsak.fk_person1, fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil, fagsak.pk_ef_fagsak_ovgst
              ,count(distinct person.fk_person1) antbarn
              ,min(nvl(case when floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12) < 0 then 0
                            else floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12)
                       end, 0)) ybarn
              ,count(distinct case when floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12) < 1
                                        then dim_person.fk_person1
                                   else null
                              end) antbu1
              ,count(distinct case when floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12) < 3
                                        then dim_person.fk_person1
                                   else null
                              end) antbu3
              ,count(distinct case when floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12) < 8
                                        then dim_person.fk_person1
                                   else null
                              end) antbu8
              ,count(distinct case when floor(months_between(fagsak.max_posteringsdato, dim_person.fodt_dato)/12) < 10
                                        then dim_person.fk_person1
                                   else null
                              end) antbu10

        from fagsak
        left outer join dvh_fam_ef.fam_ef_person person
        on fagsak.pk_ef_fagsak = person.fk_ef_fagsak
        and person.relasjon = 'BARN'

        left outer join dt_person.dim_person
        on dim_person.fk_person1 = person.fk_person1
        /*and dim_person.gyldig_fra_dato <= fagsak.vedtaks_tidspunkt
        and dim_person.gyldig_til_dato >= fagsak.vedtaks_tidspunkt*/
        and dim_person.gyldig_fra_dato <= fagsak.max_posteringsdato
        and dim_person.gyldig_til_dato >= fagsak.max_posteringsdato

        group by fagsak.periode, fagsak.fk_person1, fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil, fagsak.pk_ef_fagsak_ovgst
      ),

      fagsak_barn as (
          select fagsak.fk_person1, fagsak.kjonn, fagsak.alder, fagsak.overgkode, fagsak.ovgst overgst
                ,fagsak.barntil barntil
                ,fagsak.skolepen skolepen
                ,fagsak.fagsak_id
                ,fagsak.virk, fagsak.innt, fagsak.bruttobelop, fagsak.belop nettobelop
                ,fagsak.belop_totalt
                ,fagsak.barntil_totalt
                ,fagsak.ovgst_totalt
                ,fagsak.skolepen_totalt
                ,fagsak.aktivitetskrav
                ,fagsak.inntfradrag, fagsak.samordningsfradrag

                ,fagsak.sivst, fagsak.periode, fagsak.kommune_nr bosted_kommune_nr
                ,fagsak.bydel_kommune_nr, fagsak.aktivitet, fagsak.btdok, fagsak.antbtg, fagsak.statsb
                ,fagsak.pk_dim_person, fagsak.fodeland, fagsak.fk_dim_geografi_bosted
                ,fagsak.periode_type, fagsak.aktkode
                ,antbarn, ybarn, antbu1, antbu3, antbu8, antbu10, 'EF' kildesystem
                ,fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil, fagsak.pk_ef_fagsak_ovgst, pk_ef_fagsak_skolepen
                ,fagsak.belop_tillegg
                ,fagsak.belop_kontantstøtte
                --,sysdate lastet_dato
          from fagsak
          left join barn
          on barn.periode = fagsak.periode
          and barn.fk_person1 = fagsak.fk_person1
          and barn.pk_ef_fagsak = fagsak.pk_ef_fagsak),

        resultat as (
          select fk_person1, max(kjonn) as kjonn, max(alder) alder, max(overgkode) overgkode, max(overgst) overgst
               ,max(barntil) barntil, max(skolepen) skolepen,max(antbtg) antbtg
                ,max(virk) virk, max(innt) innt
                ,max(nvl(overgst,0))+max(nvl(barntil,0))+max(nvl(skolepen,0))+max(nvl(inntfradrag,0)) bruttobelop
                ,max(nvl(overgst,0))+max(nvl(barntil,0))+max(nvl(skolepen,0)) nettobelop
                --,sum(nettobelop) nettobelop
                --, nettobelop
                ,sum(belop_totalt) belop_totalt
                ,max(inntfradrag) inntfradrag, max(samordningsfradrag) samordningsfradrag
                ,max(btdok) btdok
                ,max(sivst) sivst, max(periode) periode, max(bosted_kommune_nr) bosted_kommune_nr
                ,max(bydel_kommune_nr) bydel_kommune_nr, max(aktivitet) aktivitet, max(statsb) statsb
                ,max(pk_dim_person) pk_dim_person, max(fodeland) fodeland, max(fk_dim_geografi_bosted) fk_dim_geografi_bosted
                ,max(periode_type) periode_type, max(aktkode) aktkode
                ,max(antbarn) antbarn, max(ybarn) ybarn, max(antbu1) antbu1, max(antbu3) antbu3, max(antbu8) antbu8, max(antbu10) antbu10, max(kildesystem) kildesystem
                ,max(pk_ef_fagsak_ovgst) pk_ef_fagsak_ovgst
                ,max(pk_ef_fagsak_barntil) pk_ef_fagsak_barntil
                ,max(pk_ef_fagsak_skolepen) pk_ef_fagsak_skolepen
                ,max(ovgst_totalt) ovgst_totalt
                ,max(barntil_totalt) barntil_totalt
                ,max(skolepen_totalt) skolepen_totalt
                ,max(belop_tillegg) belop_tillegg
                ,max(aktivitetskrav) aktivitetskrav
                ,max(belop_kontantstøtte) belop_kontantstøtte
                from fagsak_barn
				--where fk_person1 = 1019431763 --1292606586 --1800681045--
				--where fagsak.fk_person1 = '1109359276'--'1347119395' '1109359276' '1786191366'
				group by fk_person1
          )
        select /*+ PARALLEL(8) */ resultat.*
            ,'EF' as nivaa_01
            ,case when barntil > 0 and overgst is null then 'BT' else 'OG'
                end nivaa_02
            ,case when barntil > 0 and overgst is null then 'OR' else 'NY'
                end nivaa_03
        from resultat;

  begin
    l_dato_fom := to_date(p_in_period || '01', 'yyyymmdd');
    l_dato_tom := last_day(to_date(p_in_period, 'yyyymm'));
    dbms_output.put_line(l_dato_fom||'-'||l_dato_tom);--TEST!!!

    -- Slett vedtak from dvh_fam_ef.fam_ef_stonad for aktuell periode
    begin
      delete from dvh_fam_ef.fam_ef_stonad
      where kildesystem = 'EF'
      and periode = p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      commit;
    exception
      when others then
        l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, sysdate, 'FAM_EF_STONAD_INSERT1');
        commit;
        p_out_error := l_error_melding;
        l_error_melding := null;
    end;

    for rec_ef in cur_ef(l_dato_fom, l_dato_tom) loop
      begin
        insert into dvh_fam_ef.fam_ef_stonad
        (fk_person1, fk_dim_person, kjonn, alder, overgkode, ovgst, barntil--,skolepen
        , antbtg, virk, innt, bruttobelop
        ,nettobelop, inntfradrag, btdok, sivst, periode, bosted_kommune_nr, bydel_kommmune_nr
        ,aktkode, statsb, fodeland, aktivitet, vedtaks_periode_type
        ,ybarn, antbarn, antbu1, antbu3, antbu8, antbu10
        ,kildesystem, lastet_dato
        ,nivaa_01, nivaa_02, nivaa_03
        ,samordnings_belop, belop_totalt, fk_dim_geografi, fk_ef_fagsak_ovgst, fk_ef_fagsak_barntil, fk_ef_fagsak_skolepen, belop_tillegg, belop_kontantstøtte,gyldig_flagg
        ,ovgst_totalt, barntil_totalt, utd, aktivitetskrav
        )
        values
        (rec_ef.fk_person1, rec_ef.pk_dim_person, rec_ef.kjonn, rec_ef.alder, rec_ef.overgkode, rec_ef.overgst
        ,rec_ef.barntil--, rec_ef.skolepen
        ,rec_ef.antbtg, rec_ef.virk, rec_ef.innt, rec_ef.bruttobelop
        ,rec_ef.nettobelop, rec_ef.inntfradrag, rec_ef.btdok, rec_ef.sivst, rec_ef.periode, rec_ef.bosted_kommune_nr
        ,rec_ef.bydel_kommune_nr, rec_ef.aktkode, rec_ef.statsb, rec_ef.fodeland, rec_ef.aktivitet
        ,rec_ef.periode_type
        ,rec_ef.ybarn, rec_ef.antbarn, rec_ef.antbu1, rec_ef.antbu3, rec_ef.antbu8, rec_ef.antbu10
        ,rec_ef.kildesystem, sysdate
        ,rec_ef.nivaa_01, rec_ef.nivaa_02, rec_ef.nivaa_03
        ,rec_ef.samordningsfradrag, rec_ef.belop_totalt, rec_ef.fk_dim_geografi_bosted
        ,rec_ef.pk_ef_fagsak_ovgst, rec_ef.pk_ef_fagsak_barntil, rec_ef.pk_ef_fagsak_skolepen, rec_ef.belop_tillegg, rec_ef.belop_kontantstøtte,p_in_gyldig_flagg
        ,rec_ef.ovgst_totalt, rec_ef.barntil_totalt, rec_ef.skolepen_totalt ,rec_ef.aktivitetskrav);
        l_commit := l_commit + 1;
      exception
        when others then
          l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, null, l_error_melding, sysdate, 'FAM_EF_STONAD_INSERT2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
          l_error_melding := null;
      end;
      if l_commit >= 100000 then
          commit;
          l_commit := 0;
      end if;
    end loop;
    commit;
    if l_error_melding is not null then
      insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
      values(null, l_error_melding, sysdate, 'FAM_EF_STONAD_INSERT3');
      commit;
      p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
    end if;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_EF_STONAD_INSERT4');
      commit;
      p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end fam_ef_stonad_insert;

  procedure fam_ef_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm in number
                                       ,p_in_max_vedtaksperiode_yyyymm in number
                                       ,p_in_forskyvninger_dag_dd in number
                                       ,p_in_gyldig_flagg in number default 0
                                       ,p_out_error out varchar2) as
    l_error_melding varchar2(1000);
    l_commit number := 0;
    l_dato_fom date;
    l_dato_tom date;

    cursor cur_ef(p_dato_fom in date, p_dato_tom in date) is
    with versjon as
      (
        select fagsak_id
              ,max(pk_ef_fagsak) keep (dense_rank first order by vedtaks_tidspunkt desc) as pk_ef_fagsak
              ,last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd as max_vedtaksdato
        from dvh_fam_ef.fam_ef_fagsak
        where trunc(vedtaks_tidspunkt) <=
                    last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd
        and vedtaks_tidspunkt <= sysdate
        group by fagsak_id
      ),

      fagsak as
      (
        select fagsak.fagsak_id, fagsak.fk_person1, versjon.pk_ef_fagsak as pk_ef_fagsak1
              ,fagsak.pk_ef_fagsak, fagsak.behandling_type, fagsak.stonadstype
              ,fagsak.aktivitetsvilkaar_barnetilsyn aktivitetskrav
              ,fagsak.vedtaks_tidspunkt, versjon.max_vedtaksdato
              ,utbet.belop, utbet.inntektsreduksjon as inntfradrag, utbet.samordningsfradrag
              ,utbet.inntekt as innt, utbet.belop + utbet.inntektsreduksjon as bruttobelop
              ,to_char(p_dato_fom,'yyyymm') periode, utbet.delytelse_id
              ,dim_person_mottaker.pk_dim_person
              ,to_char(dim_person_mottaker.fodt_dato,'YYYY') fodsel_aar
              ,to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd
              ,dim_kjonn.kjonn_kode kjonn
              ,floor(months_between(p_dato_fom, dim_person_mottaker.fodt_dato)/12) alder
              ,case when vedtaks_perioder.periode_type = 'PERIODE_FØR_FØDSEL' then 1
                    when fagsak.stonadstype = 'OVERGANGSSTØNAD' then aktivitet_type.overgkode
               end overgkode
              ,case when fagsak.stonadstype = 'OVERGANGSSTØNAD' then utbet.belop
               end ovgst
              ,case when fagsak.stonadstype = 'BARNETILSYN' then utbet.belop
               end barntil
              ,case when fagsak.stonadstype = 'SKOLEPENGER' then utbet.belop
               end skolepen
              ,case when fagsak.stonadstype = 'OVERGANGSSTØNAD' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_ovgst
              ,case when fagsak.stonadstype = 'BARNETILSYN' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_barntil
              ,case when fagsak.stonadstype = 'SKOLEPENGER' then fagsak.pk_ef_fagsak
               end pk_ef_fagsak_skolepen
              ,(select min(utbet_forst.fra_og_med)
                from dvh_fam_ef.fam_ef_utbetalinger utbet_forst
                where utbet_forst.behandlings_id = fagsak.behandlings_id
               ) virk

              ,dim_geografi.kommune_nr, dim_geografi.bydel_kommune_nr
              ,dim_person_mottaker.statsborgerskap statsb, dim_person_mottaker.fodeland
              ,dim_person_mottaker.fk_dim_geografi_bosted
              ,sivilstatus.sivilstatus_kode sivst
              ,vedtaks_perioder.periode_type
              ,vedtaks_perioder.aktivitet

              ,case when fagsak.stonadstype = 'BARNETILSYN' then vedtaks_perioder.antallbarn
               end Antbtg,
               case when fagsak.stonadstype = 'BARNETILSYN' then vedtaks_perioder.utgifter
               end Btdok
	          ,tilleggsstonader.belop belop_tillegg
              ,kontantstøtte.belop belop_kontantstøtte
              ,aktivitet_type.aktkode
              --,sysdate
        from versjon
        join dvh_fam_ef.fam_ef_fagsak fagsak
        on versjon.pk_ef_fagsak = fagsak.pk_ef_fagsak

        join dvh_fam_ef.fam_ef_utbetalinger utbet
        on utbet.fk_ef_fagsak = fagsak.pk_ef_fagsak
        and p_dato_fom between utbet.fra_og_med and utbet.til_og_med
        and p_dato_tom between utbet.fra_og_med and utbet.til_og_med

        left outer join dt_person.dim_person dim_person_mottaker
        on dim_person_mottaker.fk_person1 = fagsak.fk_person1
        and dim_person_mottaker.gyldig_fra_dato <= fagsak.vedtaks_tidspunkt
        and dim_person_mottaker.gyldig_til_dato >= fagsak.vedtaks_tidspunkt

        left outer join dt_kodeverk.dim_sivilstatus sivilstatus
        on sivilstatus.pk_dim_sivilstatus = dim_person_mottaker.fk_dim_sivilstatus

        left outer join dt_kodeverk.dim_kjonn
        on dim_kjonn.pk_dim_kjonn = dim_person_mottaker.fk_dim_kjonn

        left outer join dt_kodeverk.dim_geografi
        on dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi

        left join dvh_fam_ef.fam_ef_vedtaksperioder vedtaks_perioder
        on vedtaks_perioder.fk_ef_fagsak = fagsak.pk_ef_fagsak
        and p_dato_fom between vedtaks_perioder.fra_og_med and vedtaks_perioder.til_og_med
        and p_dato_tom between vedtaks_perioder.fra_og_med and vedtaks_perioder.til_og_med

        left join dvh_fam_ef.fam_ef_aktivitet_type aktivitet_type
        on vedtaks_perioder.aktivitet = aktivitet_type.aktivitet


		left join dvh_fam_ef.fam_ef_tilleggsstonader tilleggsstonader
        on tilleggsstonader.fk_ef_fagsak = fagsak.pk_ef_fagsak
        and p_dato_fom between tilleggsstonader.fra_og_med and tilleggsstonader.til_og_med
        and p_dato_tom between tilleggsstonader.fra_og_med and tilleggsstonader.til_og_med
        and tilleggsstonader.TYPE_TILLEGGS_STONAD = 'TILLEGG'


        left join dvh_fam_ef.fam_ef_tilleggsstonader kontantstøtte
        on kontantstøtte.fk_ef_fagsak = fagsak.pk_ef_fagsak
        and p_dato_fom between kontantstøtte.fra_og_med and kontantstøtte.til_og_med
        and p_dato_tom between kontantstøtte.fra_og_med and kontantstøtte.til_og_med
        and kontantstøtte.TYPE_TILLEGGS_STONAD = 'KONTANTSTØTTE'

      ),
      barn as
      (
        select fagsak.periode, fagsak.fk_person1, fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil
              ,fagsak.pk_ef_fagsak_ovgst, fagsak.pk_ef_fagsak_skolepen
              ,count(distinct person.fk_person1) antbarn
              ,min(nvl(case when floor(months_between(p_dato_tom, dim_person.fodt_dato)/12) < 0 then 0
                            else floor(months_between(p_dato_tom, dim_person.fodt_dato)/12)
                       end, 0)) ybarn
              ,count(distinct case when floor(months_between(p_dato_tom, dim_person.fodt_dato)/12) < 1
                                        then dim_person.fk_person1
                                   else null
                              end) antbu1
              ,count(distinct case when floor(months_between(p_dato_tom, dim_person.fodt_dato)/12) < 3
                                        then dim_person.fk_person1
                                   else null
                              end) antbu3
              ,count(distinct case when floor(months_between(p_dato_tom, dim_person.fodt_dato)/12) < 8
                                        then dim_person.fk_person1
                                   else null
                              end) antbu8
              ,count(distinct case when floor(months_between(p_dato_tom, dim_person.fodt_dato)/12) < 10
                                        then dim_person.fk_person1
                                   else null
                              end) antbu10

        from fagsak
        left outer join dvh_fam_ef.fam_ef_person person
        on fagsak.pk_ef_fagsak = person.fk_ef_fagsak
        and person.relasjon = 'BARN'

        left outer join dt_person.dim_person dim_person
        on dim_person.fk_person1 = person.fk_person1
        and dim_person.gyldig_fra_dato <= fagsak.vedtaks_tidspunkt
        and dim_person.gyldig_til_dato >= fagsak.vedtaks_tidspunkt

        group by fagsak.periode, fagsak.fk_person1, fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil
        ,fagsak.pk_ef_fagsak_ovgst, fagsak.pk_ef_fagsak_skolepen
      ),


      fagsak_barn as (
          select fagsak.fk_person1, fagsak.kjonn, fagsak.alder, fagsak.overgkode, fagsak.ovgst overgst
                ,fagsak.barntil barntil, fagsak.skolepen skolepen
                ,fagsak.fagsak_id
                ,fagsak.aktivitetskrav
                ,fagsak.virk, fagsak.innt, fagsak.bruttobelop, fagsak.belop nettobelop
                --,fagsak.belop_totalt
                ,fagsak.inntfradrag, fagsak.samordningsfradrag

                ,fagsak.sivst, fagsak.periode, fagsak.kommune_nr bosted_kommune_nr
                ,fagsak.bydel_kommune_nr, fagsak.aktivitet, fagsak.btdok, fagsak.antbtg, fagsak.statsb
                ,fagsak.pk_dim_person, fagsak.fodeland, fagsak.fk_dim_geografi_bosted
                ,fagsak.periode_type, fagsak.aktkode,fagsak.max_vedtaksdato,fagsak.vedtaks_tidspunkt
                ,antbarn, ybarn, antbu1, antbu3, antbu8, antbu10, 'EF_VEDTAK' kildesystem
                ,fagsak.pk_ef_fagsak, fagsak.pk_ef_fagsak_barntil, fagsak.pk_ef_fagsak_ovgst, fagsak.pk_ef_fagsak_skolepen

                ,fagsak.belop_tillegg
                ,fagsak.belop_kontantstøtte
                --,sysdate lastet_dato
          from fagsak
          left join barn

          on barn.periode = fagsak.periode
          and barn.fk_person1 = fagsak.fk_person1
          and barn.pk_ef_fagsak = fagsak.pk_ef_fagsak),

     resultat as (
          select fk_person1, max(kjonn) as kjonn, max(alder) alder, max(overgkode) overgkode, max(overgst) overgst
                ,max(barntil) barntil, max(skolepen) skolepen, max(antbtg) antbtg, max(aktivitetskrav) aktivitetskrav
                ,max(virk) virk, max(innt) innt
                ,max(nvl(overgst,0))+max(nvl(barntil,0))+max(nvl(skolepen,0))+max(nvl(inntfradrag,0)) bruttobelop
                ,max(nvl(overgst,0))+max(nvl(barntil,0))+max(nvl(skolepen,0)) nettobelop
                --,sum(nettobelop) nettobelop
                --,max(nettobelop) belop_totalt
                ,max(inntfradrag) inntfradrag, max(samordningsfradrag) samordningsfradrag
                ,max(btdok) btdok, max(max_vedtaksdato) max_vedtaksdato, max(vedtaks_tidspunkt) vedtaks_tidspunkt
                ,max(sivst) sivst, max(periode) periode, max(bosted_kommune_nr) bosted_kommune_nr
                ,max(bydel_kommune_nr) bydel_kommune_nr, max(aktivitet) aktivitet, max(statsb) statsb
                ,max(pk_dim_person) pk_dim_person, max(fodeland) fodeland, max(fk_dim_geografi_bosted) fk_dim_geografi_bosted
                ,max(periode_type) periode_type, max(aktkode) aktkode
                ,max(antbarn) antbarn, max(ybarn) ybarn, max(antbu1) antbu1, max(antbu3) antbu3, max(antbu8) antbu8, max(antbu10) antbu10, max(kildesystem) kildesystem
                ,max(pk_ef_fagsak) pk_ef_fagsak
                ,max(pk_ef_fagsak_ovgst) pk_ef_fagsak_ovgst
                ,max(pk_ef_fagsak_barntil) pk_ef_fagsak_barntil
                ,max(pk_ef_fagsak_skolepen) pk_ef_fagsak_skolepen
                ,max(belop_tillegg) belop_tillegg
                ,max(belop_kontantstøtte) belop_kontantstøtte
                from fagsak_barn
				group by fk_person1
          )
        select /*+ PARALLEL(8) */ resultat.*
            ,'EF' as nivaa_01
            ,case when barntil > 0 and overgst is null then 'BT' else 'OG'
                end nivaa_02
            ,case when barntil > 0 and overgst is null then 'OR' else 'NY'
                end nivaa_03
        from resultat;

  begin
    l_dato_fom := to_date(p_in_vedtak_periode_yyyymm||'01','yyyymmdd');
    l_dato_tom := last_day(to_date(p_in_vedtak_periode_yyyymm, 'yyyymm'));
    dbms_output.put_line(l_dato_fom||'-'||l_dato_tom);--TEST!!!

    -- Slett vedtak from dvh_fam_ef.fam_ef_stonad for aktuell periode
    begin
      delete from dvh_fam_ef.fam_ef_stonad
      where kildesystem = 'EF_VEDTAK'
      and periode = p_in_vedtak_periode_yyyymm
      and gyldig_flagg = p_in_gyldig_flagg;
      commit;
    exception
      when others then
        l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, sysdate, 'FAM_EF_STONAD_VEDTAK_INSERT1');
        commit;
        p_out_error := l_error_melding;
        l_error_melding := null;
    end;

    for rec_ef in cur_ef(l_dato_fom, l_dato_tom) loop
      begin
         insert into dvh_fam_ef.fam_ef_stonad
        (fk_person1, fk_dim_person, kjonn, alder, overgkode, ovgst, barntil--, skolepen
        ,antbtg, virk, innt, bruttobelop
        ,nettobelop, inntfradrag, btdok, sivst, periode, bosted_kommune_nr, bydel_kommmune_nr
        ,fk_dim_geografi
        ,aktkode, statsb, fodeland, aktivitet, vedtaks_periode_type
        ,nivaa_01, nivaa_02, nivaa_03
        ,samordnings_belop
        ,ybarn, antbarn, antbu1, antbu3, antbu8, antbu10
        ,kildesystem, lastet_dato, vedtaks_dato, max_vedtaksdato
        ,gyldig_flagg, fk_ef_fagsak
		,fk_ef_fagsak_ovgst, fk_ef_fagsak_barntil, fk_ef_fagsak_skolepen, belop_tillegg, belop_kontantstøtte
        ,aktivitetskrav, belop_totalt, barntil_totalt, ovgst_totalt, utd
        )
        values
        (rec_ef.fk_person1, rec_ef.pk_dim_person, rec_ef.kjonn, rec_ef.alder, rec_ef.overgkode, rec_ef.overgst
        ,rec_ef.barntil--, rec_ef.skolepen
        ,rec_ef.antbtg, rec_ef.virk, rec_ef.innt, rec_ef.bruttobelop
        ,rec_ef.nettobelop, rec_ef.inntfradrag, rec_ef.btdok, rec_ef.sivst, rec_ef.periode, rec_ef.bosted_kommune_nr
        ,rec_ef.bydel_kommune_nr, rec_ef.fk_dim_geografi_bosted
        ,rec_ef.aktkode, rec_ef.statsb, rec_ef.fodeland, rec_ef.aktivitet
        ,rec_ef.periode_type
        ,rec_ef.nivaa_01, rec_ef.nivaa_02, rec_ef.nivaa_03, rec_ef.samordningsfradrag
        ,rec_ef.ybarn, rec_ef.antbarn, rec_ef.antbu1, rec_ef.antbu3, rec_ef.antbu8, rec_ef.antbu10
        ,rec_ef.kildesystem, sysdate
        ,rec_ef.vedtaks_tidspunkt, rec_ef.max_vedtaksdato
        ,p_in_gyldig_flagg, rec_ef.pk_ef_fagsak
		,rec_ef.pk_ef_fagsak_ovgst, rec_ef.pk_ef_fagsak_barntil, rec_ef.pk_ef_fagsak_skolepen
        ,rec_ef.belop_tillegg, rec_ef.belop_kontantstøtte
        ,rec_ef.aktivitetskrav, rec_ef.nettobelop, rec_ef.barntil, rec_ef.overgst, rec_ef.skolepen);
        l_commit := l_commit + 1;
      exception
        when others then
          l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_ef.pk_ef_fagsak, l_error_melding, sysdate, 'FAM_EF_STONAD_VEDTAK_INSERT2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
          l_error_melding := null;
      end;
      if l_commit >= 100000 then
          commit;
          l_commit := 0;
      end if;
    end loop;
    commit;
    if l_error_melding is not null then
      insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
      values(null, l_error_melding, sysdate, 'FAM_EF_STONAD_VEDTAK_INSERT3');
      commit;
      p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
    end if;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_EF_STONAD_VEDTAK_INSERT4');
      commit;
      p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end fam_ef_stonad_vedtak_insert;

  procedure fam_ef_patch_infotrygd_arena(p_in_periode_yyyymm in number ,p_out_error out varchar2) as
    l_error_melding varchar2(1000);
    l_commit number := 0;

    cursor cur_infotrygd is
      select fam_ef_stonad.fk_person1
            ,max(mottaker.fk_dim_person) as fk_dim_person
            ,max(mottaker.fk_dim_geografi_bosted) as fk_dim_geografi_bosted
            ,max(mottaker.bosted_kommune_nr) as bosted_kommune_nr
            ,max(mottaker.bosted_bydel_kommune_nr) as bosted_bydel_kommune_nr
            ,count(distinct barn.fkb_person1) antbarn
            ,min(nvl(case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12) < 0 then 0
                          else floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12)
                      end ,0)) ybarn
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 1 then barn.fkb_person1
                                 else null
                            end) antbu1
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 3 then barn.fkb_person1
                                 else null
                            end) antbu3
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 8 then barn.fkb_person1
                                 else null
                            end) antbu8
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 10 then barn.fkb_person1
                                 else null
                            end) antbu10
      from dvh_fam_ef.fam_ef_stonad
      left join
      (
        select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
        from
        (select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
               ,rank() over (partition by fk_person1 order by stat_aarmnd desc) as rank_nr
         from dvh_fam_bt.fam_bt_barn
         where gyldig_flagg = 1
         and stat_aarmnd <= p_in_periode_yyyymm)
        where rank_nr = 1
      ) barn
      on fam_ef_stonad.fk_person1 = barn.fk_person1

      left join
      (
        select fk_person1, fk_dim_person, fk_dim_geografi_bosted
              ,bosted_kommune_nr, bosted_bydel_kommune_nr
        from
        (select fk_person1, fk_dim_person, fk_dim_geografi_bosted
               ,bosted_kommune_nr, bosted_bydel_kommune_nr
               ,rank() over (partition by fk_person1 order by stat_aarmnd desc) as rank_nr
         from dvh_fam_bt.fam_bt_mottaker
         where gyldig_flagg = 1
         and stat_aarmnd <= p_in_periode_yyyymm)
        where rank_nr = 1
      ) mottaker
      on fam_ef_stonad.fk_person1 = mottaker.fk_person1

      where fam_ef_stonad.kildesystem = 'INFOTRYGD'
      and fam_ef_stonad.periode = p_in_periode_yyyymm
      group by fam_ef_stonad.fk_person1;

    cursor cur_arena is
      select fam_ef_stonad_arena.fk_person1
            ,max(mottaker.fk_dim_person) as fk_dim_person
            ,max(mottaker.fk_dim_geografi_bosted) as fk_dim_geografi_bosted
            ,max(mottaker.bosted_kommune_nr) as bosted_kommune_nr
            ,max(mottaker.bosted_bydel_kommune_nr) as bosted_bydel_kommune_nr
            ,count(distinct barn.fkb_person1) antbarn
            ,min(nvl(case when floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12) < 0 then 0
                          else floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12)
                      end ,0)) ybarn
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 1 then barn.fkb_person1
                                 else null
                            end) antbu1
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 3 then barn.fkb_person1
                                 else null
                            end) antbu3
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 8 then barn.fkb_person1
                                 else null
                            end) antbu8
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad_arena.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 10 then barn.fkb_person1
                                 else null
                            end) antbu10
      from dvh_fam_ef.fam_ef_stonad_arena
      left join
      (
        select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
        from
        (select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
               ,rank() over (partition by fk_person1 order by stat_aarmnd desc) as rank_nr
         from dvh_fam_bt.fam_bt_barn
         where gyldig_flagg = 1
         and stat_aarmnd <= p_in_periode_yyyymm)
        where rank_nr = 1
      ) barn
      on fam_ef_stonad_arena.fk_person1 = barn.fk_person1

      left join
      (
        select fk_person1, fk_dim_person, fk_dim_geografi_bosted
              ,bosted_kommune_nr, bosted_bydel_kommune_nr
        from
        (select fk_person1, fk_dim_person, fk_dim_geografi_bosted
               ,bosted_kommune_nr, bosted_bydel_kommune_nr
               ,rank() over (partition by fk_person1 order by stat_aarmnd desc) as rank_nr
         from dvh_fam_bt.fam_bt_mottaker
         where gyldig_flagg = 1
         and stat_aarmnd <= p_in_periode_yyyymm)
        where rank_nr = 1
      ) mottaker
      on fam_ef_stonad_arena.fk_person1 = mottaker.fk_person1

      where fam_ef_stonad_arena.periode = p_in_periode_yyyymm
      group by fam_ef_stonad_arena.fk_person1;
  begin
    for rec_infotrygd in cur_infotrygd loop
      begin
        update dvh_fam_ef.fam_ef_stonad
        set fk_dim_person = rec_infotrygd.fk_dim_person
           ,fk_dim_geografi = rec_infotrygd.fk_dim_geografi_bosted
           ,ybarn = rec_infotrygd.ybarn
           ,antbarn = rec_infotrygd.antbarn
           ,antbu1 = rec_infotrygd.antbu1
           ,antbu3 = rec_infotrygd.antbu3
           ,antbu8 = rec_infotrygd.antbu8
           ,antbu10 = rec_infotrygd.antbu10
           ,oppdatert_dato = sysdate
        where fam_ef_stonad.kildesystem = 'INFOTRYGD'
        and fam_ef_stonad.periode = p_in_periode_yyyymm
        and fk_person1 = rec_infotrygd.fk_person1;

        l_commit := l_commit + 1;
      exception
        when others then
          l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_infotrygd.fk_person1, l_error_melding, sysdate, 'FAM_EF_PATCH_INFOTRYGD_ARENA1');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
      end;

      if l_commit > 10000 then
        commit;
        l_commit := 0;
      end if;
    end loop;
    commit;

    for rec_arena in cur_arena loop
      begin
        update dvh_fam_ef.fam_ef_stonad_arena
        set fk_dim_person = rec_arena.fk_dim_person
           ,fk_dim_geografi = rec_arena.fk_dim_geografi_bosted
           ,ybarn = rec_arena.ybarn
           ,antbarn = rec_arena.antbarn
           ,antbu1 = rec_arena.antbu1
           ,antbu3 = rec_arena.antbu3
           ,antbu8 = rec_arena.antbu8
           ,antbu10 = rec_arena.antbu10
           ,oppdatert_dato = sysdate
        where fam_ef_stonad_arena.periode = p_in_periode_yyyymm
        and fk_person1 = rec_arena.fk_person1;

        l_commit := l_commit + 1;
      exception
        when others then
          l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_arena.fk_person1, l_error_melding, sysdate, 'FAM_EF_PATCH_INFOTRYGD_ARENA2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
      end;

      if l_commit > 10000 then
        commit;
        l_commit := 0;
      end if;
    end loop;
    commit;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_EF_PATCH_INFOTRYGD_ARENA3');
      commit;
      p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end fam_ef_patch_infotrygd_arena;

  procedure fam_ef_patch_migrering_vedtak(p_in_periode_yyyymm in number ,p_out_error out varchar2) as
    cursor cur_stonad is
      select fam_ef_stonad.fk_person1
            ,max(kode.max_overgkode) as max_overgkode, max(kode.max_aktkode) as max_aktkode
            ,count(distinct barn.fkb_person1) antbarn
            ,min(nvl(case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12) < 0 then 0
                          else floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                   ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                   )/12)
                      end ,0)) ybarn
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 1 then barn.fkb_person1
                                 else null
                            end) antbu1
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 3 then barn.fkb_person1
                                 else null
                            end) antbu3
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 8 then barn.fkb_person1
                                 else null
                            end) antbu8
            ,count(distinct case when floor(months_between(last_day(to_date(fam_ef_stonad.periode,'yyyymm'))
                                                          ,last_day(to_date(barn.fodsel_aar_barn||barn.fodsel_mnd_barn,'yyyymm'))
                                                          )/12) < 10 then barn.fkb_person1
                                 else null
                            end) antbu10
      from dvh_fam_ef.fam_ef_stonad
      left join
      (
        select fk_person1
              ,max(overgkode) keep (dense_rank first order by periode desc) as max_overgkode
              ,max(aktkode) keep (dense_rank first order by periode desc) as max_aktkode
        from dvh_fam_ef.fam_ef_stonad
        where kildesystem = 'INFOTRYGD'
        and ovgst > 0
        group by fk_person1
      ) kode
     on fam_ef_stonad.fk_person1 = kode.fk_person1

     left join
     (
       select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
       from
       (select fk_person1, fkb_person1, fodsel_aar_barn, fodsel_mnd_barn
              ,rank() over (partition by fk_person1 order by stat_aarmnd desc) as rank_nr
        from dvh_fam_bt.fam_bt_barn
        where gyldig_flagg = 1
        and stat_aarmnd <= p_in_periode_yyyymm)
       where rank_nr = 1
     ) barn
     on fam_ef_stonad.fk_person1 = barn.fk_person1

     where fam_ef_stonad.antbarn = 0
     --fam_ef_stonad.aktivitet = 'MIGRERING'
     and fam_ef_stonad.kildesystem != 'INFOTRYGD'
     and fam_ef_stonad.gyldig_flagg = 1
     and fam_ef_stonad.periode = p_in_periode_yyyymm
     --and fam_ef_stonad.fk_person1 = :p_in_fk_person1
     group by fam_ef_stonad.fk_person1;

    v_overgkode varchar2(10);
    v_aktkode varchar2(10);
    v_error varchar2(1000);
  begin
    for rec_stonad in cur_stonad loop
      begin
        update dvh_fam_ef.fam_ef_stonad
        set overgkode = rec_stonad.max_overgkode
           ,aktkode = rec_stonad.max_aktkode
           ,antbarn = rec_stonad.antbarn
           ,ybarn = rec_stonad.ybarn
           ,antbu1 = rec_stonad.antbu1
           ,antbu3 = rec_stonad.antbu3
           ,antbu8 = rec_stonad.antbu8
           ,antbu10 = rec_stonad.antbu10
           ,oppdatert_dato = sysdate
        where antbarn = 0
        --aktivitet = 'MIGRERING'
        and kildesystem != 'INFOTRYGD'
        and gyldig_flagg = 1
        and periode = p_in_periode_yyyymm
        and fk_person1 = rec_stonad.fk_person1;
      exception
        when others then
          v_error := substr(sqlcode || sqlerrm, 1, 1000);
          insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_stonad.fk_person1, v_error, sysdate, 'FAM_EF_PATCH_MIGRERING_VEDTAK1');
          p_out_error := substr(p_out_error || v_error, 1, 1000);
      end;
    end loop;
    commit;
  exception
    when others then
      v_error := substr(sqlcode || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error, sysdate, 'FAM_EF_PATCH_MIGRERING_VEDTAK2');
      p_out_error := substr(p_out_error || v_error, 1, 1000);
  end fam_ef_patch_migrering_vedtak;

end fam_ef;