create or replace PACKAGE BODY                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         FAM_FP AS

  function dim_tid_antall(p_in_tid_fom in number, p_in_tid_tom in number) 
    return number as
    v_dim_tid_antall number := 0;
  begin
    select count(1)
    into v_dim_tid_antall
    from dt_kodeverk.dim_tid
    where dag_i_uke < 6   
    and dim_nivaa = 1
    and gyldig_flagg = 1
    and pk_dim_tid between p_in_tid_fom and p_in_tid_tom;
    return v_dim_tid_antall;
  exception
    when others then
      return 0;
  end;

  procedure fam_fp_statistikk_maaned(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'M'
                                     ,p_out_error out varchar2) as
    cursor cur_periode(p_rapport_dato in varchar2, p_forskyvninger in number
                      ,p_tid_fom in varchar2, p_tid_tom in varchar2
                      ,p_budsjett in varchar2) is
      with fagsak as
      (
        select fagsak_id, max(behandlingstema) as behandlingstema, max(fagsakannenforelder_id) as annenforelderfagsak_id
              ,max(trans_id) keep(dense_rank first order by funksjonell_tid desc) as max_trans_id
              ,max(soeknadsdato) keep(dense_rank first order by funksjonell_tid desc) as soknadsdato
              ,min(soeknadsdato) as forste_soknadsdato
              ,min(vedtaksdato) as forste_vedtaksdato
              ,max(funksjonell_tid) as funksjonell_tid, max(vedtaksdato) as siste_vedtaksdato
              ,p_in_periode_type as periode, last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger as max_vedtaksdato
        from fk_sensitiv.fam_fp_fagsak        
        where fam_fp_fagsak.funksjonell_tid <= last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger
        group by fagsak_id
      ),
      termin as
      (
        select fagsak_id, max(termindato) termindato, max(foedselsdato) foedselsdato
              ,max(antall_barn_termin) antall_barn_termin, max(antall_barn_foedsel) antall_barn_foedsel
              ,max(foedselsdato_adopsjon) foedselsdato_adopsjon, max(antall_barn_adopsjon) antall_barn_adopsjon
        from
        (
          select fam_fp_fagsak.fagsak_id, max(fodsel.termindato) termindato
                ,max(fodsel.foedselsdato) foedselsdato, max(fodsel.antall_barn_foedsel) antall_barn_foedsel
                ,max(fodsel.antall_barn_termin) antall_barn_termin
                ,max(adopsjon.foedselsdato_adopsjon) foedselsdato_adopsjon
                ,count(adopsjon.trans_id) antall_barn_adopsjon
          from fk_sensitiv.fam_fp_fagsak
          left join fk_sensitiv.fam_fp_fodseltermin fodsel
          on fodsel.fagsak_id = fam_fp_fagsak.fagsak_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_FODS'
          left join fk_sensitiv.fam_fp_fodseltermin adopsjon
          on adopsjon.fagsak_id = fam_fp_fagsak.fagsak_id
          and adopsjon.trans_id = fam_fp_fagsak.trans_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_ADOP'
          group by fam_fp_fagsak.fagsak_id, fam_fp_fagsak.trans_id
        )
        group by fagsak_id
      ),
      fk_person1 as
      (
        select person.person, person.fagsak_id, max(person.behandlingstema) as behandlingstema, person.max_trans_id
              ,max(person.annenforelderfagsak_id) as annenforelderfagsak_id
              ,person.aktoer_id, max(person.kjonn) as kjonn
              ,max(person_67_vasket.fk_person1) keep
                (dense_rank first order by person_67_vasket.gyldig_fra_dato desc) as fk_person1
              ,max(foedselsdato) as foedselsdato, max(sivilstand) as sivilstand
              ,max(statsborgerskap) as statsborgerskap
        from
        (
          select 'MOTTAKER' as person, fagsak.fagsak_id, fagsak.behandlingstema, fagsak.max_trans_id
                ,fagsak.annenforelderfagsak_id
                ,fam_fp_personopplysninger.aktoer_id, fam_fp_personopplysninger.kjonn
                ,fam_fp_personopplysninger.foedselsdato, fam_fp_personopplysninger.sivilstand
                ,fam_fp_personopplysninger.statsborgerskap
          from fk_sensitiv.fam_fp_personopplysninger
          join fagsak
          on fam_fp_personopplysninger.trans_id = fagsak.max_trans_id
          union all
          select 'BARN' as person, fagsak.fagsak_id, max(fagsak.behandlingstema) as behandlingstema, fagsak.max_trans_id
                ,max(fagsak.annenforelderfagsak_id) annenforelderfagsak_id
                ,max(fam_fp_familiehendelse.til_aktoer_id) as aktoer_id, max(fam_fp_familiehendelse.kjoenn) as kjonn
                ,null as foedselsdato, null as sivilstand, null as statsborgerskap
          from fk_sensitiv.fam_fp_familiehendelse
          join fagsak
          on fam_fp_familiehendelse.fagsak_id = fagsak.fagsak_id
          where upper(fam_fp_familiehendelse.relasjon) = 'BARN'
          group by fagsak.fagsak_id, fagsak.max_trans_id
        ) person
        join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
        on person_67_vasket.aktor_id = person.aktoer_id
        group by person.person, person.fagsak_id, person.max_trans_id, person.aktoer_id
      ),
      barn as
      (
        select fagsak_id, listagg(fk_person1, ',') within group (order by fk_person1) as fk_person1_barn
        from fk_person1
        where person = 'BARN'
        group by fagsak_id
      ),
      mottaker as
      (
        select fk_person1.fagsak_id, fk_person1.behandlingstema
              ,fk_person1.max_trans_id, fk_person1.annenforelderfagsak_id, fk_person1.aktoer_id
              ,fk_person1.kjonn, fk_person1.fk_person1 as fk_person1_mottaker
              ,extract(year from fk_person1.foedselsdato) as mottaker_fodsels_aar
              ,extract(month from fk_person1.foedselsdato) as mottaker_fodsels_mnd
              ,fk_person1.sivilstand, fk_person1.statsborgerskap
              ,barn.fk_person1_barn
              ,termin.termindato, termin.foedselsdato, termin.antall_barn_termin, termin.antall_barn_foedsel
              ,termin.foedselsdato_adopsjon, termin.antall_barn_adopsjon
        from fk_person1
        left join barn
        on barn.fagsak_id = fk_person1.fagsak_id
        left join termin
        on fk_person1.fagsak_id = termin.fagsak_id
        where fk_person1.person = 'MOTTAKER'
      ),
      adopsjon as
      (
        select fam_fp_vilkaar.fagsak_id
              ,max(fam_fp_vilkaar.omsorgs_overtakelsesdato) as adopsjonsdato
              ,max(fam_fp_vilkaar.ektefelles_barn) as stebarnsadopsjon
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.fagsak_id = fam_fp_vilkaar.fagsak_id
        where fagsak.behandlingstema = 'FORP_ADOP'
        group by fam_fp_vilkaar.fagsak_id
      ),
      eos as
      (
       select a.trans_id
             ,case when upper(er_borger_av_eu_eos) = 'TRUE' then 'J'
                   when upper(er_borger_av_eu_eos) = 'FALSE' then 'N'
				  		    else null
              end	eos_sak
       from
       (select fam_fp_vilkaar.trans_id, max(fam_fp_vilkaar.er_borger_av_eu_eos) as er_borger_av_eu_eos
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.max_trans_id = fam_fp_vilkaar.trans_id
        and length(fam_fp_vilkaar.person_status) > 0
        group by fam_fp_vilkaar.trans_id
       ) a
      ),
      annenforelderfagsak as
      (
        select annenforelderfagsak.*, mottaker.fk_person1_mottaker as fk_person1_annen_part
        from
        (
          select fagsak_id, max_trans_id, max(annenforelderfagsak_id) as annenforelderfagsak_id
          from
          (
            select forelder1.fagsak_id, forelder1.max_trans_id
                  ,nvl(forelder1.annenforelderfagsak_id, forelder2.fagsak_id) as annenforelderfagsak_id
            from mottaker forelder1
            join mottaker forelder2
            on forelder1.fk_person1_barn = forelder2.fk_person1_barn
            and forelder1.fk_person1_mottaker != forelder2.fk_person1_mottaker
          )
          group by fagsak_id, max_trans_id
        ) annenforelderfagsak
        join mottaker
        on annenforelderfagsak.annenforelderfagsak_id = mottaker.fagsak_id
      ),
      tid as
      (
        select pk_dim_tid, dato, aar, halvaar, kvartal, aar_maaned
        from dt_kodeverk.dim_tid
        where dag_i_uke < 6   
        and dim_nivaa = 1
        and gyldig_flagg = 1
        and pk_dim_tid between p_tid_fom and p_tid_tom
        and ((p_budsjett = 'A' and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd'))
             or p_budsjett = 'B')
      ),
      uttak as
      (
        select uttak.trans_id, uttak.trekkonto, uttak.uttak_arbeid_type, uttak.virksomhet, uttak.utbetalingsprosent
              ,uttak.gradering_innvilget, uttak.gradering, uttak.arbeidstidsprosent, uttak.samtidig_uttak
              ,uttak.periode_resultat_aarsak, uttak.fom as uttak_fom, uttak.tom as uttak_tom
              ,uttak.trekkdager
              ,fagsak.fagsak_id, fagsak.periode, fagsak.funksjonell_tid, fagsak.forste_vedtaksdato, fagsak.siste_vedtaksdato
              ,fagsak.max_vedtaksdato, fagsak.forste_soknadsdato, fagsak.soknadsdato
              ,fam_fp_trekkonto.pk_fam_fp_trekkonto
              ,aarsak_uttak.pk_fam_fp_periode_resultat_aarsak
              ,uttak.arbeidsforhold_id, uttak.graderingsdager
              ,fam_fp_uttak_fordelingsper.mors_aktivitet
         from fk_sensitiv.fam_fp_uttak_res_per_aktiv uttak
         join fagsak
         on fagsak.max_trans_id = uttak.trans_id
         left join dvh_fam_fp.fam_fp_trekkonto
         on upper(uttak.trekkonto) = fam_fp_trekkonto.trekkonto
         left join
         (select aarsak_uttak, max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
          from dvh_fam_fp.fam_fp_periode_resultat_aarsak
          group by aarsak_uttak
         ) aarsak_uttak
         on upper(uttak.periode_resultat_aarsak) = aarsak_uttak.aarsak_uttak
         left join fk_sensitiv.fam_fp_uttak_fordelingsper
         on fam_fp_uttak_fordelingsper.trans_id = uttak.trans_id
         and uttak.fom between fam_fp_uttak_fordelingsper.fom and fam_fp_uttak_fordelingsper.tom
         and upper(uttak.trekkonto) = upper(fam_fp_uttak_fordelingsper.periode_type)
         and length(fam_fp_uttak_fordelingsper.mors_aktivitet) > 1
         where uttak.utbetalingsprosent > 0
      ),
      stonadsdager_kvote as
      (
        select uttak.*, tid1.pk_dim_tid as fk_dim_tid_min_dato_kvote
              ,tid2.pk_dim_tid as fk_dim_tid_max_dato_kvote
        from
        (select fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
               ,sum(trekkdager) as stonadsdager_kvote, min(uttak_fom) as min_uttak_fom
               ,max(uttak_tom) as max_uttak_tom
         from
         (select fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
                ,max(trekkdager) as trekkdager
          from uttak
          group by fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
         ) a
         group by fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
        ) uttak
        join dt_kodeverk.dim_tid tid1
        on tid1.dim_nivaa = 1
        and tid1.dato = trunc(uttak.min_uttak_fom,'dd')
        join dt_kodeverk.dim_tid tid2
        on tid2.dim_nivaa = 1
        and tid2.dato = trunc(uttak.max_uttak_tom,'dd')
      ),
      uttak_dager AS 
      (
        select uttak.*
              ,tid.pk_dim_tid, tid.dato, tid.aar, tid.halvaar, tid.kvartal, tid.aar_maaned              
        from uttak
        join tid
        on tid.dato between uttak.uttak_fom and uttak.uttak_tom
      ),
      aleneomsorg as
      (
        select uttak.fagsak_id, uttak.uttak_fom
        from uttak
        join fk_sensitiv.fam_fp_dokumentasjonsperioder dok1
        on dok1.fagsak_id = uttak.fagsak_id
        and uttak.uttak_fom >= dok1.fom
        and dok1.dokumentasjon_type = 'ALENEOMSORG'
        left join fk_sensitiv.fam_fp_dokumentasjonsperioder dok2
        on dok1.fagsak_id = dok2.fagsak_id
        and uttak.uttak_fom >= dok2.fom
        and dok1.trans_id < dok2.trans_id
        and dok2.dokumentasjon_type = 'ANNEN_FORELDER_HAR_RETT'
        and dok2.fagsak_id is null
        group by uttak.fagsak_id, uttak.uttak_fom
      ),
      beregningsgrunnlag as
      (
        select fagsak_id, trans_id, virksomhetsnummer, max(status_og_andel_brutto) as status_og_andel_brutto
              ,max(status_og_andel_avkortet) as status_og_andel_avkortet
              ,fom as beregningsgrunnlag_fom, tom as beregningsgrunnlag_tom
              ,max(dekningsgrad) as dekningsgrad, max(dagsats) as dagsats, dagsats_bruker
              ,dagsats_arbeidsgiver
              ,dagsats_bruker+dagsats_arbeidsgiver dagsats_virksomhet
              ,max(status_og_andel_inntektskat) as status_og_andel_inntektskat
              ,aktivitet_status, max(brutto) as brutto_inntekt, max(avkortet) as avkortet_inntekt
              ,count(1) as antall_beregningsgrunnlag
        from fk_sensitiv.fam_fp_beregningsgrunnlag
        group by fagsak_id, trans_id, virksomhetsnummer, fom, tom, aktivitet_status, dagsats_bruker, dagsats_arbeidsgiver
      ),
      beregningsgrunnlag_detalj as
      (
        select uttak_dager.*
              ,stonadsdager_kvote.stonadsdager_kvote, stonadsdager_kvote.min_uttak_fom, stonadsdager_kvote.max_uttak_tom
              ,stonadsdager_kvote.fk_dim_tid_min_dato_kvote, stonadsdager_kvote.fk_dim_tid_max_dato_kvote
              ,bereg.status_og_andel_brutto, bereg.status_og_andel_avkortet, bereg.beregningsgrunnlag_fom
              ,bereg.dekningsgrad, bereg.beregningsgrunnlag_tom, bereg.dagsats, bereg.dagsats_bruker
              ,bereg.dagsats_arbeidsgiver
              ,bereg.dagsats_virksomhet, bereg.status_og_andel_inntektskat
              ,bereg.aktivitet_status, bereg.brutto_inntekt, bereg.avkortet_inntekt
              ,bereg.dagsats*uttak_dager.utbetalingsprosent/100 as dagsats_erst
              ,bereg.antall_beregningsgrunnlag
        from beregningsgrunnlag bereg
        join uttak_dager
        on uttak_dager.trans_id = bereg.trans_id
        and nvl(uttak_dager.virksomhet,'X') = nvl(bereg.virksomhetsnummer,'X')
        and bereg.beregningsgrunnlag_fom <= uttak_dager.dato
        and nvl(bereg.beregningsgrunnlag_tom,to_date('20991201','YYYYMMDD')) >= uttak_dager.dato
        left join stonadsdager_kvote
        on uttak_dager.trans_id = stonadsdager_kvote.trans_id
        and uttak_dager.trekkonto = stonadsdager_kvote.trekkonto
        and nvl(uttak_dager.virksomhet,'X') = nvl(stonadsdager_kvote.virksomhet,'X')
        and uttak_dager.uttak_arbeid_type = stonadsdager_kvote.uttak_arbeid_type
        join dvh_fam_fp.fam_fp_uttak_aktivitet_mapping uttak_mapping
        on uttak_dager.uttak_arbeid_type = uttak_mapping.uttak_arbeid
        and bereg.aktivitet_status = uttak_mapping.aktivitet_status
        where bereg.dagsats_bruker + bereg.dagsats_arbeidsgiver != 0
      ),
      beregningsgrunnlag_agg as
      (
        select a.*
              ,dager_erst*dagsats_virksomhet/dagsats*antall_beregningsgrunnlag tilfelle_erst
              ,dager_erst*round(utbetalingsprosent/100*dagsats_virksomhet) belop
              ,round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert
              ,case when periode_resultat_aarsak in (2004,2033) then 'N'	
			 	            when trekkonto in ('FEDREKVOTE','FELLESPERIODE','MØDREKVOTE') then 'J'
				            when trekkonto = 'FORELDREPENGER' then 'N'
			         end mor_rettighet
        from
        (
          select fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                ,aar, halvaar, kvartal, aar_maaned
                ,uttak_fom, uttak_tom
                ,sum(dagsats_virksomhet/dagsats* case when ((upper(gradering_innvilget) ='TRUE' and upper(gradering)='TRUE') 
                                       or upper(samtidig_uttak)='TRUE') then (100-arbeidstidsprosent)/100
                               else 1.0
                          end
                     ) dager_erst2
                ,max(arbeidstidsprosent) as arbeidstidsprosent
                ,count(distinct pk_dim_tid) dager_erst
                ,
                 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
                 min(beregningsgrunnlag_fom) beregningsgrunnlag_fom, max(beregningsgrunnlag_tom) beregningsgrunnlag_tom
                ,dekningsgrad
                ,
                 --count(distinct pk_dim_tid)*
                 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
                 dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                ,virksomhet, periode_resultat_aarsak, dagsats, dagsats_erst
                , --dagsats_virksomhet,
                 utbetalingsprosent graderingsprosent, status_og_andel_inntektskat
                ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
                ,
                 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
                 utbetalingsprosent
                ,min(pk_dim_tid) pk_dim_tid_dato_utbet_fom, max(pk_dim_tid) pk_dim_tid_dato_utbet_tom
                ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                ,max(forste_soknadsdato) as forste_soknadsdato, max(soknadsdato) as soknadsdato
                ,samtidig_uttak, gradering, gradering_innvilget
                ,min_uttak_fom, max_uttak_tom
                ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                ,max(pk_fam_fp_trekkonto) as pk_fam_fp_trekkonto
                ,max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
                ,antall_beregningsgrunnlag, max(graderingsdager) as graderingsdager
                ,max(mors_aktivitet) as mors_aktivitet
          from beregningsgrunnlag_detalj
          group by fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                  ,aar, halvaar, kvartal, aar_maaned
                  ,uttak_fom, uttak_tom, dekningsgrad
                  ,virksomhet, utbetalingsprosent, periode_resultat_aarsak
                  ,dagsats, dagsats_erst, dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                  ,utbetalingsprosent
                  ,status_og_andel_inntektskat, aktivitet_status, brutto_inntekt, avkortet_inntekt
                  ,status_og_andel_brutto, status_og_andel_avkortet
                  ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                  ,samtidig_uttak, gradering, gradering_innvilget, min_uttak_fom, max_uttak_tom
                  ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                  ,antall_beregningsgrunnlag
        ) a
      ),
      grunnlag as
      (
        select beregningsgrunnlag_agg.*, sysdate as lastet_dato
              ,mottaker.behandlingstema, mottaker.max_trans_id, mottaker.fk_person1_mottaker, mottaker.kjonn
              ,mottaker.fk_person1_barn
              ,mottaker.termindato, mottaker.foedselsdato, mottaker.antall_barn_termin
              ,mottaker.antall_barn_foedsel, mottaker.foedselsdato_adopsjon
              ,mottaker.antall_barn_adopsjon
              ,mottaker.mottaker_fodsels_aar, mottaker.mottaker_fodsels_mnd
              ,substr(p_tid_fom,1,4) - mottaker.mottaker_fodsels_aar as mottaker_alder
              ,mottaker.sivilstand, mottaker.statsborgerskap
              ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
              ,dim_person.fk_dim_sivilstatus
              ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr, dim_geografi.kommune_nr
              ,dim_geografi.kommune_navn, dim_geografi.bydel_nr, dim_geografi.bydel_navn
              ,annenforelderfagsak.annenforelderfagsak_id, annenforelderfagsak.fk_person1_annen_part
              ,fam_fp_uttak_fp_kontoer.max_dager max_stonadsdager_konto
              ,case when aleneomsorg.fagsak_id is not null then 'J' else NULL end as aleneomsorg
              ,case when behandlingstema = 'FORP_FODS' then '214'
                    when behandlingstema = 'FORP_ADOP' then '216'
               end as hovedkontonr
              ,case when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100<=50 then '1000'
               when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100>50 then '8020'
               --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
               when status_og_andel_inntektskat='JORDBRUKER' then '5210'
               when status_og_andel_inntektskat='SJØMANN' then '1300'
               when status_og_andel_inntektskat='SELVSTENDIG_NÆRINGSDRIVENDE' then '5010'
               when status_og_andel_inntektskat='DAGPENGER' then '1200'
               when status_og_andel_inntektskat='ARBEIDSTAKER_UTEN_FERIEPENGER' then '1000'
               when status_og_andel_inntektskat='FISKER' then '5300'
               when status_og_andel_inntektskat='DAGMAMMA' then '5110'
               when status_og_andel_inntektskat='FRILANSER' then '1100'
               end as underkontonr
              ,case when rett_til_mødrekvote.trans_id is null then 'N' else 'J' end as rett_til_mødrekvote
              ,case when rett_til_fedrekvote.trans_id is null then 'N' else 'J' end as rett_til_fedrekvote
              ,flerbarnsdager.flerbarnsdager
              ,round(dagsats_arbeidsgiver/dagsats*100,0) as andel_av_refusjon
              ,adopsjon.adopsjonsdato, adopsjon.stebarnsadopsjon
              ,eos.eos_sak
        from beregningsgrunnlag_agg
        left join mottaker
        on beregningsgrunnlag_agg.fagsak_id = mottaker.fagsak_id
        and beregningsgrunnlag_agg.trans_id = mottaker.max_trans_id
        left join annenforelderfagsak
        on beregningsgrunnlag_agg.fagsak_id = annenforelderfagsak.fagsak_id
        and beregningsgrunnlag_agg.trans_id = annenforelderfagsak.max_trans_id
        left join fk_sensitiv.fam_fp_uttak_fp_kontoer
        on beregningsgrunnlag_agg.fagsak_id = fam_fp_uttak_fp_kontoer.fagsak_id
        and mottaker.max_trans_id = fam_fp_uttak_fp_kontoer.trans_id
        --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
        and upper(replace(beregningsgrunnlag_agg.trekkonto,'_','')) = upper(replace(fam_fp_uttak_fp_kontoer.stoenadskontotype,' ',''))
        left join dt_person.dim_person
        on mottaker.fk_person1_mottaker = dim_person.fk_person1
        and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
        left join dt_kodeverk.dim_geografi
        on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        left join aleneomsorg
        on aleneomsorg.fagsak_id = beregningsgrunnlag_agg.fagsak_id
        and aleneomsorg.uttak_fom = beregningsgrunnlag_agg.uttak_fom
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'MØDREKVOTE'
         group by trans_id
        ) rett_til_mødrekvote
        on rett_til_mødrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FEDREKVOTE'
         group by trans_id
        ) rett_til_fedrekvote
        on rett_til_fedrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id, max(max_dager) as flerbarnsdager
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FLERBARNSDAGER'
         group by trans_id
        ) flerbarnsdager
        on flerbarnsdager.trans_id = beregningsgrunnlag_agg.trans_id
        left join adopsjon
        on beregningsgrunnlag_agg.fagsak_id = adopsjon.fagsak_id
        left join eos
        on beregningsgrunnlag_agg.trans_id = eos.trans_id
      )
      select /*+ PARALLEL(8) */ *
      --from uttak_dager
      from grunnlag
      --where fagsak_id in (1035184)
      ;
    v_tid_fom varchar2(8) := null;
    v_tid_tom varchar2(8) := null;
    v_commit number := 0;
    v_error_melding varchar2(1000) := null;
    v_dim_tid_antall number := 0;
    v_utbetalingsprosent_kalkulert number := 0;
    v_budsjett varchar2(5);
  begin
    v_tid_fom := p_in_vedtak_tom || '01';
    v_tid_tom := to_char(last_day(to_date(p_in_vedtak_tom,'yyyymm')),'yyyymmdd');
    if to_date(p_in_vedtak_tom,'yyyymm') <= to_date(p_in_rapport_dato,'yyyymm') then
      v_budsjett := 'A';
    else
      v_budsjett := 'B';
    end if;

    --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!

    for rec_periode in cur_periode(p_in_rapport_dato, p_in_forskyvninger, v_tid_fom, v_tid_tom, v_budsjett) loop
      v_dim_tid_antall := 0;
      v_utbetalingsprosent_kalkulert := 0;
      v_dim_tid_antall := dim_tid_antall(to_number(to_char(rec_periode.uttak_fom,'yyyymmdd'))
                                        ,to_number(to_char(rec_periode.uttak_tom,'yyyymmdd')));
      if v_dim_tid_antall != 0 then                                      
        v_utbetalingsprosent_kalkulert := round(rec_periode.trekkdager/v_dim_tid_antall*100,2);
      else
        v_utbetalingsprosent_kalkulert := 0;
      end if;
      begin
        insert into dvh_fam_fp.fam_fp_vedtak_utbetaling
        (fagsak_id, trans_id, behandlingstema, trekkonto, stonadsdager_kvote
        ,uttak_arbeid_type
        ,aar, halvaar, kvartal, aar_maaned
        ,rapport_periode, uttak_fom, uttak_tom, dager_erst, beregningsgrunnlag_fom
        ,beregningsgrunnlag_tom, dekningsgrad, dagsats_bruker, dagsats_arbeidsgiver, virksomhet
        ,periode_resultat_aarsak, dagsats, graderingsprosent, status_og_andel_inntektskat
        ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
        ,utbetalingsprosent, fk_dim_tid_dato_utbet_fom
        ,fk_dim_tid_dato_utbet_tom, funksjonell_tid, forste_vedtaksdato, vedtaksdato, max_vedtaksdato, periode_type, tilfelle_erst
        ,belop, dagsats_redusert, lastet_dato, max_trans_id, fk_person1_mottaker, fk_person1_annen_part
        ,kjonn, fk_person1_barn, termindato, foedselsdato, antall_barn_termin, antall_barn_foedsel
        ,foedselsdato_adopsjon, antall_barn_adopsjon, annenforelderfagsak_id, max_stonadsdager_konto
        ,fk_dim_person, bosted_kommune_nr, fk_dim_geografi, bydel_kommune_nr, kommune_nr
        ,kommune_navn, bydel_nr, bydel_navn, aleneomsorg, hovedkontonr, underkontonr
        ,mottaker_fodsels_aar, mottaker_fodsels_mnd, mottaker_alder
        ,rett_til_fedrekvote, rett_til_modrekvote, dagsats_erst, trekkdager
        ,samtidig_uttak, gradering, gradering_innvilget, antall_dager_periode
        ,flerbarnsdager, utbetalingsprosent_kalkulert, min_uttak_fom, max_uttak_tom
        ,fk_fam_fp_trekkonto, fk_fam_fp_periode_resultat_aarsak
        ,sivilstatus, fk_dim_sivilstatus, antall_beregningsgrunnlag, graderingsdager
        ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
        ,adopsjonsdato, stebarnsadopsjon, eos_sak, mor_rettighet, statsborgerskap
        ,arbeidstidsprosent, mors_aktivitet, gyldig_flagg
        ,andel_av_refusjon, forste_soknadsdato, soknadsdato
        ,budsjett)
        values
        (rec_periode.fagsak_id, rec_periode.trans_id, rec_periode.behandlingstema, rec_periode.trekkonto
        ,rec_periode.stonadsdager_kvote
        ,rec_periode.uttak_arbeid_type, rec_periode.aar, rec_periode.halvaar, rec_periode.kvartal, rec_periode.aar_maaned
        ,p_in_rapport_dato, rec_periode.uttak_fom, rec_periode.uttak_tom
        ,rec_periode.dager_erst, rec_periode.beregningsgrunnlag_fom, rec_periode.beregningsgrunnlag_tom
        ,rec_periode.dekningsgrad, rec_periode.dagsats_bruker, rec_periode.dagsats_arbeidsgiver
        ,rec_periode.virksomhet, rec_periode.periode_resultat_aarsak, rec_periode.dagsats
        ,rec_periode.graderingsprosent, rec_periode.status_og_andel_inntektskat
        ,rec_periode.aktivitet_status, rec_periode.brutto_inntekt, rec_periode.avkortet_inntekt
        ,rec_periode.status_og_andel_brutto, rec_periode.status_og_andel_avkortet
        ,rec_periode.utbetalingsprosent, rec_periode.pk_dim_tid_dato_utbet_fom, rec_periode.pk_dim_tid_dato_utbet_tom
        ,rec_periode.funksjonell_tid, rec_periode.forste_vedtaksdato, rec_periode.siste_vedtaksdato, rec_periode.max_vedtaksdato, rec_periode.periode
        ,rec_periode.tilfelle_erst, rec_periode.belop, rec_periode.dagsats_redusert, rec_periode.lastet_dato
        ,rec_periode.max_trans_id, rec_periode.fk_person1_mottaker, rec_periode.fk_person1_annen_part
        ,rec_periode.kjonn, rec_periode.fk_person1_barn
        ,rec_periode.termindato, rec_periode.foedselsdato, rec_periode.antall_barn_termin
        ,rec_periode.antall_barn_foedsel, rec_periode.foedselsdato_adopsjon
        ,rec_periode.antall_barn_adopsjon, rec_periode.annenforelderfagsak_id, rec_periode.max_stonadsdager_konto
        ,rec_periode.pk_dim_person, rec_periode.bosted_kommune_nr, rec_periode.pk_dim_geografi
        ,rec_periode.bydel_kommune_nr, rec_periode.kommune_nr, rec_periode.kommune_navn
        ,rec_periode.bydel_nr, rec_periode.bydel_navn, rec_periode.aleneomsorg, rec_periode.hovedkontonr
        ,rec_periode.underkontonr
        ,rec_periode.mottaker_fodsels_aar, rec_periode.mottaker_fodsels_mnd, rec_periode.mottaker_alder
        ,rec_periode.rett_til_fedrekvote, rec_periode.rett_til_mødrekvote, rec_periode.dagsats_erst
        ,rec_periode.trekkdager, rec_periode.samtidig_uttak, rec_periode.gradering, rec_periode.gradering_innvilget
        ,v_dim_tid_antall, rec_periode.flerbarnsdager, v_utbetalingsprosent_kalkulert
        ,rec_periode.min_uttak_fom, rec_periode.max_uttak_tom, rec_periode.pk_fam_fp_trekkonto
        ,rec_periode.pk_fam_fp_periode_resultat_aarsak
        ,rec_periode.sivilstand, rec_periode.fk_dim_sivilstatus
        ,rec_periode.antall_beregningsgrunnlag, rec_periode.graderingsdager
        ,rec_periode.fk_dim_tid_min_dato_kvote, rec_periode.fk_dim_tid_max_dato_kvote
        ,rec_periode.adopsjonsdato, rec_periode.stebarnsadopsjon, rec_periode.eos_sak
        ,rec_periode.mor_rettighet, rec_periode.statsborgerskap
        ,rec_periode.arbeidstidsprosent, rec_periode.mors_aktivitet, p_in_gyldig_flagg
        ,rec_periode.andel_av_refusjon, rec_periode.forste_soknadsdato, rec_periode.soknadsdato
        ,v_budsjett);

        v_commit := v_commit + 1;
      exception
        when others then
          rollback;
          v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_periode.fagsak_id, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_MAANED:INSERT');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;

      if v_commit > 100000 then
        commit;
        v_commit := 0;
      end if;
   end loop;
   commit;
  exception
    when others then
      rollback;
      v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_MAANED');
      commit;
      p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
  end fam_fp_statistikk_maaned;

  procedure fam_fp_statistikk_kvartal(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'K'
                                     ,p_out_error out varchar2) as
    cursor cur_periode(p_rapport_dato in varchar2, p_forskyvninger in number, p_tid_fom in varchar2, p_tid_tom in varchar2) is
      with fagsak as
      (
        select fagsak_id, max(behandlingstema) as behandlingstema, max(fagsakannenforelder_id) as annenforelderfagsak_id
              ,max(trans_id) keep(dense_rank first order by funksjonell_tid desc) as max_trans_id
              ,max(soeknadsdato) keep(dense_rank first order by funksjonell_tid desc) as soknadsdato
              ,min(soeknadsdato) as forste_soknadsdato
              ,min(vedtaksdato) as forste_vedtaksdato
              ,max(funksjonell_tid) as funksjonell_tid, max(vedtaksdato) as siste_vedtaksdato
              ,p_in_periode_type as periode, last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger as max_vedtaksdato
        from fk_sensitiv.fam_fp_fagsak        
        where fam_fp_fagsak.funksjonell_tid <= last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger
        group by fagsak_id
      ),
      termin as
      (
        select fagsak_id, max(termindato) termindato, max(foedselsdato) foedselsdato
              ,max(antall_barn_termin) antall_barn_termin, max(antall_barn_foedsel) antall_barn_foedsel
              ,max(foedselsdato_adopsjon) foedselsdato_adopsjon, max(antall_barn_adopsjon) antall_barn_adopsjon
        from
        (
          select fam_fp_fagsak.fagsak_id, max(fodsel.termindato) termindato
                ,max(fodsel.foedselsdato) foedselsdato, max(fodsel.antall_barn_foedsel) antall_barn_foedsel
                ,max(fodsel.antall_barn_termin) antall_barn_termin
                ,max(adopsjon.foedselsdato_adopsjon) foedselsdato_adopsjon
                ,count(adopsjon.trans_id) antall_barn_adopsjon
          from fk_sensitiv.fam_fp_fagsak
          left join fk_sensitiv.fam_fp_fodseltermin fodsel
          on fodsel.fagsak_id = fam_fp_fagsak.fagsak_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_FODS'
          left join fk_sensitiv.fam_fp_fodseltermin adopsjon
          on adopsjon.fagsak_id = fam_fp_fagsak.fagsak_id
          and adopsjon.trans_id = fam_fp_fagsak.trans_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_ADOP'
          group by fam_fp_fagsak.fagsak_id, fam_fp_fagsak.trans_id
        )
        group by fagsak_id
      ),
      fk_person1 as
      (
        select person.person, person.fagsak_id, max(person.behandlingstema) as behandlingstema, person.max_trans_id
              ,max(person.annenforelderfagsak_id) as annenforelderfagsak_id
              ,person.aktoer_id, max(person.kjonn) as kjonn
              ,max(person_67_vasket.fk_person1) keep
                (dense_rank first order by person_67_vasket.gyldig_fra_dato desc) as fk_person1
              ,max(foedselsdato) as foedselsdato, max(sivilstand) as sivilstand
              ,max(statsborgerskap) as statsborgerskap
        from
        (
          select 'MOTTAKER' as person, fagsak.fagsak_id, fagsak.behandlingstema, fagsak.max_trans_id
                ,fagsak.annenforelderfagsak_id
                ,fam_fp_personopplysninger.aktoer_id, fam_fp_personopplysninger.kjonn
                ,fam_fp_personopplysninger.foedselsdato, fam_fp_personopplysninger.sivilstand
                ,fam_fp_personopplysninger.statsborgerskap
          from fk_sensitiv.fam_fp_personopplysninger
          join fagsak
          on fam_fp_personopplysninger.trans_id = fagsak.max_trans_id
          union all
          select 'BARN' as person, fagsak.fagsak_id, max(fagsak.behandlingstema) as behandlingstema, fagsak.max_trans_id
                ,max(fagsak.annenforelderfagsak_id) annenforelderfagsak_id
                ,max(fam_fp_familiehendelse.til_aktoer_id) as aktoer_id, max(fam_fp_familiehendelse.kjoenn) as kjonn
                ,null as foedselsdato, null as sivilstand, null as statsborgerskap
          from fk_sensitiv.fam_fp_familiehendelse
          join fagsak
          on fam_fp_familiehendelse.fagsak_id = fagsak.fagsak_id
          where upper(fam_fp_familiehendelse.relasjon) = 'BARN'
          group by fagsak.fagsak_id, fagsak.max_trans_id
        ) person
        join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
        on person_67_vasket.aktor_id = person.aktoer_id
        group by person.person, person.fagsak_id, person.max_trans_id, person.aktoer_id
      ),
      barn as
      (
        select fagsak_id, listagg(fk_person1, ',') within group (order by fk_person1) as fk_person1_barn
        from fk_person1
        where person = 'BARN'
        group by fagsak_id
      ),
      mottaker as
      (
        select fk_person1.fagsak_id, fk_person1.behandlingstema
              ,fk_person1.max_trans_id, fk_person1.annenforelderfagsak_id, fk_person1.aktoer_id
              ,fk_person1.kjonn, fk_person1.fk_person1 as fk_person1_mottaker
              ,extract(year from fk_person1.foedselsdato) as mottaker_fodsels_aar
              ,extract(month from fk_person1.foedselsdato) as mottaker_fodsels_mnd
              ,fk_person1.sivilstand, fk_person1.statsborgerskap
              ,barn.fk_person1_barn
              ,termin.termindato, termin.foedselsdato, termin.antall_barn_termin, termin.antall_barn_foedsel
              ,termin.foedselsdato_adopsjon, termin.antall_barn_adopsjon
        from fk_person1
        left join barn
        on barn.fagsak_id = fk_person1.fagsak_id
        left join termin
        on fk_person1.fagsak_id = termin.fagsak_id
        where fk_person1.person = 'MOTTAKER'
      ),
      adopsjon as
      (
        select fam_fp_vilkaar.fagsak_id
              ,max(fam_fp_vilkaar.omsorgs_overtakelsesdato) as adopsjonsdato
              ,max(fam_fp_vilkaar.ektefelles_barn) as stebarnsadopsjon
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.fagsak_id = fam_fp_vilkaar.fagsak_id
        where fagsak.behandlingstema = 'FORP_ADOP'
        group by fam_fp_vilkaar.fagsak_id
      ),
      eos as
      (
       select a.trans_id
             ,case when upper(er_borger_av_eu_eos) = 'TRUE' then 'J'
                   when upper(er_borger_av_eu_eos) = 'FALSE' then 'N'
				  		    else null
              end	eos_sak
       from
       (select fam_fp_vilkaar.trans_id, max(fam_fp_vilkaar.er_borger_av_eu_eos) as er_borger_av_eu_eos
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.max_trans_id = fam_fp_vilkaar.trans_id
        and length(fam_fp_vilkaar.person_status) > 0
        group by fam_fp_vilkaar.trans_id
       ) a
      ),
      annenforelderfagsak as
      (
        select annenforelderfagsak.*, mottaker.fk_person1_mottaker as fk_person1_annen_part
        from
        (
          select fagsak_id, max_trans_id, max(annenforelderfagsak_id) as annenforelderfagsak_id
          from
          (
            select forelder1.fagsak_id, forelder1.max_trans_id
                  ,nvl(forelder1.annenforelderfagsak_id, forelder2.fagsak_id) as annenforelderfagsak_id
            from mottaker forelder1
            join mottaker forelder2
            on forelder1.fk_person1_barn = forelder2.fk_person1_barn
            and forelder1.fk_person1_mottaker != forelder2.fk_person1_mottaker
          )
          group by fagsak_id, max_trans_id
        ) annenforelderfagsak
        join mottaker
        on annenforelderfagsak.annenforelderfagsak_id = mottaker.fagsak_id
      ),
      tid as
      (
        select pk_dim_tid, dato, aar, halvaar, kvartal, aar_maaned
        from dt_kodeverk.dim_tid
        where dag_i_uke < 6   
        and dim_nivaa = 1
        and gyldig_flagg = 1
        and pk_dim_tid between p_tid_fom and p_tid_tom
        and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd')
      ),
      uttak as
      (
        select uttak.trans_id, uttak.trekkonto, uttak.uttak_arbeid_type, uttak.virksomhet, uttak.utbetalingsprosent
              ,uttak.gradering_innvilget, uttak.gradering, uttak.arbeidstidsprosent, uttak.samtidig_uttak
              ,uttak.periode_resultat_aarsak, uttak.fom as uttak_fom, uttak.tom as uttak_tom
              ,uttak.trekkdager
              ,fagsak.fagsak_id, fagsak.periode, fagsak.funksjonell_tid, fagsak.forste_vedtaksdato, fagsak.siste_vedtaksdato
              ,fagsak.max_vedtaksdato, fagsak.forste_soknadsdato, fagsak.soknadsdato
              ,fam_fp_trekkonto.pk_fam_fp_trekkonto
              ,aarsak_uttak.pk_fam_fp_periode_resultat_aarsak
              ,uttak.arbeidsforhold_id, uttak.graderingsdager
              ,fam_fp_uttak_fordelingsper.mors_aktivitet
         from fk_sensitiv.fam_fp_uttak_res_per_aktiv uttak
         join fagsak
         on fagsak.max_trans_id = uttak.trans_id
         left join dvh_fam_fp.fam_fp_trekkonto
         on upper(uttak.trekkonto) = fam_fp_trekkonto.trekkonto
         left join
         (select aarsak_uttak, max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
          from dvh_fam_fp.fam_fp_periode_resultat_aarsak
          group by aarsak_uttak
         ) aarsak_uttak
         on upper(uttak.periode_resultat_aarsak) = aarsak_uttak.aarsak_uttak
         left join fk_sensitiv.fam_fp_uttak_fordelingsper
         on fam_fp_uttak_fordelingsper.trans_id = uttak.trans_id
         and uttak.fom between fam_fp_uttak_fordelingsper.fom and fam_fp_uttak_fordelingsper.tom
         and upper(uttak.trekkonto) = upper(fam_fp_uttak_fordelingsper.periode_type)
         and length(fam_fp_uttak_fordelingsper.mors_aktivitet) > 1
         where uttak.utbetalingsprosent > 0
      ),
      stonadsdager_kvote as
      (
        select uttak.*, tid1.pk_dim_tid as fk_dim_tid_min_dato_kvote
              ,tid2.pk_dim_tid as fk_dim_tid_max_dato_kvote
        from
        (select fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
               ,sum(trekkdager) as stonadsdager_kvote, min(uttak_fom) as min_uttak_fom
               ,max(uttak_tom) as max_uttak_tom
         from
         (select fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
                ,max(trekkdager) as trekkdager
          from uttak
          group by fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
         ) a
         group by fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
        ) uttak
        join dt_kodeverk.dim_tid tid1
        on tid1.dim_nivaa = 1
        and tid1.dato = trunc(uttak.min_uttak_fom,'dd')
        join dt_kodeverk.dim_tid tid2
        on tid2.dim_nivaa = 1
        and tid2.dato = trunc(uttak.max_uttak_tom,'dd')
      ),
      uttak_dager AS 
      (
        select uttak.*
              ,tid.pk_dim_tid, tid.dato, tid.aar, tid.halvaar, tid.kvartal, tid.aar_maaned              
        from uttak
        join tid
        on tid.dato between uttak.uttak_fom and uttak.uttak_tom
      ),
      aleneomsorg as
      (
        select uttak.fagsak_id, uttak.uttak_fom
        from uttak
        join fk_sensitiv.fam_fp_dokumentasjonsperioder dok1
        on dok1.fagsak_id = uttak.fagsak_id
        and uttak.uttak_fom >= dok1.fom
        and dok1.dokumentasjon_type = 'ALENEOMSORG'
        left join fk_sensitiv.fam_fp_dokumentasjonsperioder dok2
        on dok1.fagsak_id = dok2.fagsak_id
        and uttak.uttak_fom >= dok2.fom
        and dok1.trans_id < dok2.trans_id
        and dok2.dokumentasjon_type = 'ANNEN_FORELDER_HAR_RETT'
        and dok2.fagsak_id is null
        group by uttak.fagsak_id, uttak.uttak_fom
      ),
      beregningsgrunnlag as
      (
        select fagsak_id, trans_id, virksomhetsnummer, max(status_og_andel_brutto) as status_og_andel_brutto
              ,max(status_og_andel_avkortet) as status_og_andel_avkortet
              ,fom as beregningsgrunnlag_fom, tom as beregningsgrunnlag_tom
              ,max(dekningsgrad) as dekningsgrad, max(dagsats) as dagsats, dagsats_bruker
              ,dagsats_arbeidsgiver
              ,dagsats_bruker+dagsats_arbeidsgiver dagsats_virksomhet
              ,max(status_og_andel_inntektskat) as status_og_andel_inntektskat
              ,aktivitet_status, max(brutto) as brutto_inntekt, max(avkortet) as avkortet_inntekt
              ,count(1) as antall_beregningsgrunnlag
        from fk_sensitiv.fam_fp_beregningsgrunnlag
        group by fagsak_id, trans_id, virksomhetsnummer, fom, tom, aktivitet_status, dagsats_bruker, dagsats_arbeidsgiver
      ),
      beregningsgrunnlag_detalj as
      (
        select uttak_dager.*
              ,stonadsdager_kvote.stonadsdager_kvote, stonadsdager_kvote.min_uttak_fom, stonadsdager_kvote.max_uttak_tom
              ,stonadsdager_kvote.fk_dim_tid_min_dato_kvote, stonadsdager_kvote.fk_dim_tid_max_dato_kvote
              ,bereg.status_og_andel_brutto, bereg.status_og_andel_avkortet, bereg.beregningsgrunnlag_fom
              ,bereg.dekningsgrad, bereg.beregningsgrunnlag_tom, bereg.dagsats, bereg.dagsats_bruker
              ,bereg.dagsats_arbeidsgiver
              ,bereg.dagsats_virksomhet, bereg.status_og_andel_inntektskat
              ,bereg.aktivitet_status, bereg.brutto_inntekt, bereg.avkortet_inntekt
              ,bereg.dagsats*uttak_dager.utbetalingsprosent/100 as dagsats_erst
              ,bereg.antall_beregningsgrunnlag
        from beregningsgrunnlag bereg
        join uttak_dager
        on uttak_dager.trans_id = bereg.trans_id
        and nvl(uttak_dager.virksomhet,'X') = nvl(bereg.virksomhetsnummer,'X')
        and bereg.beregningsgrunnlag_fom <= uttak_dager.dato
        and nvl(bereg.beregningsgrunnlag_tom,to_date('20991201','YYYYMMDD')) >= uttak_dager.dato
        left join stonadsdager_kvote
        on uttak_dager.trans_id = stonadsdager_kvote.trans_id
        and uttak_dager.trekkonto = stonadsdager_kvote.trekkonto
        and nvl(uttak_dager.virksomhet,'X') = nvl(stonadsdager_kvote.virksomhet,'X')
        and uttak_dager.uttak_arbeid_type = stonadsdager_kvote.uttak_arbeid_type
        join dvh_fam_fp.fam_fp_uttak_aktivitet_mapping uttak_mapping
        on uttak_dager.uttak_arbeid_type = uttak_mapping.uttak_arbeid
        and bereg.aktivitet_status = uttak_mapping.aktivitet_status
        where bereg.dagsats_bruker + bereg.dagsats_arbeidsgiver != 0
      ),
      beregningsgrunnlag_agg as
      (
        select a.*
              ,dager_erst*dagsats_virksomhet/dagsats*antall_beregningsgrunnlag tilfelle_erst
              ,dager_erst*round(utbetalingsprosent/100*dagsats_virksomhet) belop
              ,round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert
              ,case when periode_resultat_aarsak in (2004,2033) then 'N'	
			 	            when trekkonto in ('FEDREKVOTE','FELLESPERIODE','MØDREKVOTE') then 'J'
				            when trekkonto = 'FORELDREPENGER' then 'N'
			         end mor_rettighet
        from
        (
          select fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                ,aar, halvaar, kvartal--, aar_maaned
                ,uttak_fom, uttak_tom
                ,sum(dagsats_virksomhet/dagsats* case when ((upper(gradering_innvilget) ='TRUE' and upper(gradering)='TRUE') 
                                       or upper(samtidig_uttak)='TRUE') then (100-arbeidstidsprosent)/100
                               else 1.0
                          end
                     ) dager_erst2
                ,max(arbeidstidsprosent) as arbeidstidsprosent
                ,count(distinct pk_dim_tid) dager_erst
                ,
                 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
                 min(beregningsgrunnlag_fom) beregningsgrunnlag_fom, max(beregningsgrunnlag_tom) beregningsgrunnlag_tom
                ,dekningsgrad
                ,
                 --count(distinct pk_dim_tid)*
                 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
                 dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                ,virksomhet, periode_resultat_aarsak, dagsats, dagsats_erst
                , --dagsats_virksomhet,
                 utbetalingsprosent graderingsprosent, status_og_andel_inntektskat
                ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
                ,
                 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
                 utbetalingsprosent
                ,min(pk_dim_tid) pk_dim_tid_dato_utbet_fom, max(pk_dim_tid) pk_dim_tid_dato_utbet_tom
                ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                ,max(forste_soknadsdato) as forste_soknadsdato, max(soknadsdato) as soknadsdato
                ,samtidig_uttak, gradering, gradering_innvilget
                ,min_uttak_fom, max_uttak_tom
                ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                ,max(pk_fam_fp_trekkonto) as pk_fam_fp_trekkonto
                ,max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
                ,antall_beregningsgrunnlag, max(graderingsdager) as graderingsdager
                ,max(mors_aktivitet) as mors_aktivitet
          from beregningsgrunnlag_detalj
          group by fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                  ,aar, halvaar, kvartal--, aar_maaned
                  ,uttak_fom, uttak_tom, dekningsgrad
                  ,virksomhet, utbetalingsprosent, periode_resultat_aarsak
                  ,dagsats, dagsats_erst, dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                  ,utbetalingsprosent
                  ,status_og_andel_inntektskat, aktivitet_status, brutto_inntekt, avkortet_inntekt
                  ,status_og_andel_brutto, status_og_andel_avkortet
                  ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                  ,samtidig_uttak, gradering, gradering_innvilget, min_uttak_fom, max_uttak_tom
                  ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                  ,antall_beregningsgrunnlag
        ) a
      ),
      grunnlag as
      (
        select beregningsgrunnlag_agg.*, sysdate as lastet_dato
              ,mottaker.behandlingstema, mottaker.max_trans_id, mottaker.fk_person1_mottaker, mottaker.kjonn
              ,mottaker.fk_person1_barn
              ,mottaker.termindato, mottaker.foedselsdato, mottaker.antall_barn_termin
              ,mottaker.antall_barn_foedsel, mottaker.foedselsdato_adopsjon
              ,mottaker.antall_barn_adopsjon
              ,mottaker.mottaker_fodsels_aar, mottaker.mottaker_fodsels_mnd
              ,substr(p_tid_fom,1,4) - mottaker.mottaker_fodsels_aar as mottaker_alder
              ,mottaker.sivilstand, mottaker.statsborgerskap
              ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
              ,dim_person.fk_dim_sivilstatus
              ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr, dim_geografi.kommune_nr
              ,dim_geografi.kommune_navn, dim_geografi.bydel_nr, dim_geografi.bydel_navn
              ,annenforelderfagsak.annenforelderfagsak_id, annenforelderfagsak.fk_person1_annen_part
              ,fam_fp_uttak_fp_kontoer.max_dager max_stonadsdager_konto
              ,case when aleneomsorg.fagsak_id is not null then 'J' else NULL end as aleneomsorg
              ,case when behandlingstema = 'FORP_FODS' then '214'
                    when behandlingstema = 'FORP_ADOP' then '216'
               end as hovedkontonr
              ,case when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100<=50 then '1000'
               when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100>50 then '8020'
               --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
               when status_og_andel_inntektskat='JORDBRUKER' then '5210'
               when status_og_andel_inntektskat='SJØMANN' then '1300'
               when status_og_andel_inntektskat='SELVSTENDIG_NÆRINGSDRIVENDE' then '5010'
               when status_og_andel_inntektskat='DAGPENGER' then '1200'
               when status_og_andel_inntektskat='ARBEIDSTAKER_UTEN_FERIEPENGER' then '1000'
               when status_og_andel_inntektskat='FISKER' then '5300'
               when status_og_andel_inntektskat='DAGMAMMA' then '5110'
               when status_og_andel_inntektskat='FRILANSER' then '1100'
               end as underkontonr
              ,round(dagsats_arbeidsgiver/dagsats*100,0) as andel_av_refusjon
              ,case when rett_til_mødrekvote.trans_id is null then 'N' else 'J' end as rett_til_mødrekvote
              ,case when rett_til_fedrekvote.trans_id is null then 'N' else 'J' end as rett_til_fedrekvote
              ,flerbarnsdager.flerbarnsdager
              ,adopsjon.adopsjonsdato, adopsjon.stebarnsadopsjon
              ,eos.eos_sak
        from beregningsgrunnlag_agg
        left join mottaker
        on beregningsgrunnlag_agg.fagsak_id = mottaker.fagsak_id
        and beregningsgrunnlag_agg.trans_id = mottaker.max_trans_id
        left join annenforelderfagsak
        on beregningsgrunnlag_agg.fagsak_id = annenforelderfagsak.fagsak_id
        and beregningsgrunnlag_agg.trans_id = annenforelderfagsak.max_trans_id
        left join fk_sensitiv.fam_fp_uttak_fp_kontoer
        on beregningsgrunnlag_agg.fagsak_id = fam_fp_uttak_fp_kontoer.fagsak_id
        and mottaker.max_trans_id = fam_fp_uttak_fp_kontoer.trans_id
        --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
        and upper(replace(beregningsgrunnlag_agg.trekkonto,'_','')) = upper(replace(fam_fp_uttak_fp_kontoer.stoenadskontotype,' ',''))
        left join dt_person.dim_person
        on mottaker.fk_person1_mottaker = dim_person.fk_person1
        and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
        left join dt_kodeverk.dim_geografi
        on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        left join aleneomsorg
        on aleneomsorg.fagsak_id = beregningsgrunnlag_agg.fagsak_id
        and aleneomsorg.uttak_fom = beregningsgrunnlag_agg.uttak_fom
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'MØDREKVOTE'
         group by trans_id
        ) rett_til_mødrekvote
        on rett_til_mødrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FEDREKVOTE'
         group by trans_id
        ) rett_til_fedrekvote
        on rett_til_fedrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id, max(max_dager) as flerbarnsdager
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FLERBARNSDAGER'
         group by trans_id
        ) flerbarnsdager
        on flerbarnsdager.trans_id = beregningsgrunnlag_agg.trans_id
        left join adopsjon
        on beregningsgrunnlag_agg.fagsak_id = adopsjon.fagsak_id
        left join eos
        on beregningsgrunnlag_agg.trans_id = eos.trans_id
      )
      select /*+ PARALLEL(8) */ *
      --from uttak_dager
      from grunnlag
      --where fagsak_id in (1035184)
      ;
    v_tid_fom varchar2(8) := null;
    v_tid_tom varchar2(8) := null;
    v_commit number := 0;
    v_error_melding varchar2(1000) := null;
    v_dim_tid_antall number := 0;
    v_utbetalingsprosent_kalkulert number := 0;
  begin
    v_tid_fom := substr(p_in_vedtak_tom,1,4) || substr(p_in_vedtak_tom,5,6)-2 || '01';
    v_tid_tom := to_char(last_day(to_date(p_in_vedtak_tom,'yyyymm')),'yyyymmdd');

    --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!

    for rec_periode in cur_periode(p_in_rapport_dato, p_in_forskyvninger, v_tid_fom, v_tid_tom) loop
      v_dim_tid_antall := 0;
      v_utbetalingsprosent_kalkulert := 0;
      v_dim_tid_antall := dim_tid_antall(to_number(to_char(rec_periode.uttak_fom,'yyyymmdd'))
                                        ,to_number(to_char(rec_periode.uttak_tom,'yyyymmdd')));
      if v_dim_tid_antall != 0 then                                      
        v_utbetalingsprosent_kalkulert := round(rec_periode.trekkdager/v_dim_tid_antall*100,2);
      else
        v_utbetalingsprosent_kalkulert := 0;
      end if;
      begin
        insert into dvh_fam_fp.fam_fp_vedtak_utbetaling
        (fagsak_id, trans_id, behandlingstema, trekkonto, stonadsdager_kvote
        ,uttak_arbeid_type
        ,aar, halvaar, kvartal--, aar_maaned
        ,rapport_periode, uttak_fom, uttak_tom, dager_erst, beregningsgrunnlag_fom
        ,beregningsgrunnlag_tom, dekningsgrad, dagsats_bruker, dagsats_arbeidsgiver, virksomhet
        ,periode_resultat_aarsak, dagsats, graderingsprosent, status_og_andel_inntektskat
        ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
        ,utbetalingsprosent, fk_dim_tid_dato_utbet_fom
        ,fk_dim_tid_dato_utbet_tom, funksjonell_tid, forste_vedtaksdato, vedtaksdato, max_vedtaksdato, periode_type, tilfelle_erst
        ,belop, dagsats_redusert, lastet_dato, max_trans_id, fk_person1_mottaker, fk_person1_annen_part
        ,kjonn, fk_person1_barn, termindato, foedselsdato, antall_barn_termin, antall_barn_foedsel
        ,foedselsdato_adopsjon, antall_barn_adopsjon, annenforelderfagsak_id, max_stonadsdager_konto
        ,fk_dim_person, bosted_kommune_nr, fk_dim_geografi, bydel_kommune_nr, kommune_nr
        ,kommune_navn, bydel_nr, bydel_navn, aleneomsorg, hovedkontonr, underkontonr
        ,mottaker_fodsels_aar, mottaker_fodsels_mnd, mottaker_alder
        ,rett_til_fedrekvote, rett_til_modrekvote, dagsats_erst, trekkdager
        ,samtidig_uttak, gradering, gradering_innvilget, antall_dager_periode
        ,flerbarnsdager, utbetalingsprosent_kalkulert, min_uttak_fom, max_uttak_tom
        ,fk_fam_fp_trekkonto, fk_fam_fp_periode_resultat_aarsak
        ,sivilstatus, fk_dim_sivilstatus, antall_beregningsgrunnlag, graderingsdager
        ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
        ,adopsjonsdato, stebarnsadopsjon, eos_sak, mor_rettighet, statsborgerskap
        ,arbeidstidsprosent, mors_aktivitet, gyldig_flagg
        ,andel_av_refusjon, forste_soknadsdato, soknadsdato)
        values
        (rec_periode.fagsak_id, rec_periode.trans_id, rec_periode.behandlingstema, rec_periode.trekkonto
        ,rec_periode.stonadsdager_kvote
        ,rec_periode.uttak_arbeid_type, rec_periode.aar, rec_periode.halvaar, rec_periode.kvartal--, rec_periode.aar_maaned
        ,p_in_rapport_dato, rec_periode.uttak_fom, rec_periode.uttak_tom
        ,rec_periode.dager_erst, rec_periode.beregningsgrunnlag_fom, rec_periode.beregningsgrunnlag_tom
        ,rec_periode.dekningsgrad, rec_periode.dagsats_bruker, rec_periode.dagsats_arbeidsgiver
        ,rec_periode.virksomhet, rec_periode.periode_resultat_aarsak, rec_periode.dagsats
        ,rec_periode.graderingsprosent, rec_periode.status_og_andel_inntektskat
        ,rec_periode.aktivitet_status, rec_periode.brutto_inntekt, rec_periode.avkortet_inntekt
        ,rec_periode.status_og_andel_brutto, rec_periode.status_og_andel_avkortet
        ,rec_periode.utbetalingsprosent, rec_periode.pk_dim_tid_dato_utbet_fom, rec_periode.pk_dim_tid_dato_utbet_tom
        ,rec_periode.funksjonell_tid, rec_periode.forste_vedtaksdato, rec_periode.siste_vedtaksdato, rec_periode.max_vedtaksdato, rec_periode.periode
        ,rec_periode.tilfelle_erst, rec_periode.belop, rec_periode.dagsats_redusert, rec_periode.lastet_dato
        ,rec_periode.max_trans_id, rec_periode.fk_person1_mottaker, rec_periode.fk_person1_annen_part
        ,rec_periode.kjonn, rec_periode.fk_person1_barn
        ,rec_periode.termindato, rec_periode.foedselsdato, rec_periode.antall_barn_termin
        ,rec_periode.antall_barn_foedsel, rec_periode.foedselsdato_adopsjon
        ,rec_periode.antall_barn_adopsjon, rec_periode.annenforelderfagsak_id, rec_periode.max_stonadsdager_konto
        ,rec_periode.pk_dim_person, rec_periode.bosted_kommune_nr, rec_periode.pk_dim_geografi
        ,rec_periode.bydel_kommune_nr, rec_periode.kommune_nr, rec_periode.kommune_navn
        ,rec_periode.bydel_nr, rec_periode.bydel_navn, rec_periode.aleneomsorg, rec_periode.hovedkontonr
        ,rec_periode.underkontonr
        ,rec_periode.mottaker_fodsels_aar, rec_periode.mottaker_fodsels_mnd, rec_periode.mottaker_alder
        ,rec_periode.rett_til_fedrekvote, rec_periode.rett_til_mødrekvote, rec_periode.dagsats_erst
        ,rec_periode.trekkdager, rec_periode.samtidig_uttak, rec_periode.gradering, rec_periode.gradering_innvilget
        ,v_dim_tid_antall, rec_periode.flerbarnsdager, v_utbetalingsprosent_kalkulert
        ,rec_periode.min_uttak_fom, rec_periode.max_uttak_tom, rec_periode.pk_fam_fp_trekkonto
        ,rec_periode.pk_fam_fp_periode_resultat_aarsak
        ,rec_periode.sivilstand, rec_periode.fk_dim_sivilstatus
        ,rec_periode.antall_beregningsgrunnlag, rec_periode.graderingsdager
        ,rec_periode.fk_dim_tid_min_dato_kvote, rec_periode.fk_dim_tid_max_dato_kvote
        ,rec_periode.adopsjonsdato, rec_periode.stebarnsadopsjon, rec_periode.eos_sak
        ,rec_periode.mor_rettighet, rec_periode.statsborgerskap
        ,rec_periode.arbeidstidsprosent, rec_periode.mors_aktivitet, p_in_gyldig_flagg
        ,rec_periode.andel_av_refusjon, rec_periode.forste_soknadsdato, rec_periode.soknadsdato);

        v_commit := v_commit + 1;
      exception
        when others then
          rollback;
          v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_periode.fagsak_id, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_KVARTAL:INSERT');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;

      if v_commit > 100000 then
        commit;
        v_commit := 0;
      end if;
   end loop;
   commit;
  exception
    when others then
      rollback;
      v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_KVARTAL');
      commit;
      p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
  end fam_fp_statistikk_kvartal;

  procedure fam_fp_statistikk_halvaar(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'H'
                                     ,p_out_error out varchar2) as
    cursor cur_periode(p_rapport_dato in varchar2, p_forskyvninger in number, p_tid_fom in varchar2, p_tid_tom in varchar2) is
      with fagsak as
      (
        select fagsak_id, max(behandlingstema) as behandlingstema, max(fagsakannenforelder_id) as annenforelderfagsak_id
              ,max(trans_id) keep(dense_rank first order by funksjonell_tid desc) as max_trans_id
              ,max(soeknadsdato) keep(dense_rank first order by funksjonell_tid desc) as soknadsdato
              ,min(soeknadsdato) as forste_soknadsdato
              ,min(vedtaksdato) as forste_vedtaksdato
              ,max(funksjonell_tid) as funksjonell_tid, max(vedtaksdato) as siste_vedtaksdato
              ,p_in_periode_type as periode, last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger as max_vedtaksdato
        from fk_sensitiv.fam_fp_fagsak        
        where fam_fp_fagsak.funksjonell_tid <= last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger
        group by fagsak_id
      ),
      termin as
      (
        select fagsak_id, max(termindato) termindato, max(foedselsdato) foedselsdato
              ,max(antall_barn_termin) antall_barn_termin, max(antall_barn_foedsel) antall_barn_foedsel
              ,max(foedselsdato_adopsjon) foedselsdato_adopsjon, max(antall_barn_adopsjon) antall_barn_adopsjon
        from
        (
          select fam_fp_fagsak.fagsak_id, max(fodsel.termindato) termindato
                ,max(fodsel.foedselsdato) foedselsdato, max(fodsel.antall_barn_foedsel) antall_barn_foedsel
                ,max(fodsel.antall_barn_termin) antall_barn_termin
                ,max(adopsjon.foedselsdato_adopsjon) foedselsdato_adopsjon
                ,count(adopsjon.trans_id) antall_barn_adopsjon
          from fk_sensitiv.fam_fp_fagsak
          left join fk_sensitiv.fam_fp_fodseltermin fodsel
          on fodsel.fagsak_id = fam_fp_fagsak.fagsak_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_FODS'
          left join fk_sensitiv.fam_fp_fodseltermin adopsjon
          on adopsjon.fagsak_id = fam_fp_fagsak.fagsak_id
          and adopsjon.trans_id = fam_fp_fagsak.trans_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_ADOP'
          group by fam_fp_fagsak.fagsak_id, fam_fp_fagsak.trans_id
        )
        group by fagsak_id
      ),
      fk_person1 as
      (
        select person.person, person.fagsak_id, max(person.behandlingstema) as behandlingstema, person.max_trans_id
              ,max(person.annenforelderfagsak_id) as annenforelderfagsak_id
              ,person.aktoer_id, max(person.kjonn) as kjonn
              ,max(person_67_vasket.fk_person1) keep
                (dense_rank first order by person_67_vasket.gyldig_fra_dato desc) as fk_person1
              ,max(foedselsdato) as foedselsdato, max(sivilstand) as sivilstand
              ,max(statsborgerskap) as statsborgerskap
        from
        (
          select 'MOTTAKER' as person, fagsak.fagsak_id, fagsak.behandlingstema, fagsak.max_trans_id
                ,fagsak.annenforelderfagsak_id
                ,fam_fp_personopplysninger.aktoer_id, fam_fp_personopplysninger.kjonn
                ,fam_fp_personopplysninger.foedselsdato, fam_fp_personopplysninger.sivilstand
                ,fam_fp_personopplysninger.statsborgerskap
          from fk_sensitiv.fam_fp_personopplysninger
          join fagsak
          on fam_fp_personopplysninger.trans_id = fagsak.max_trans_id
          union all
          select 'BARN' as person, fagsak.fagsak_id, max(fagsak.behandlingstema) as behandlingstema, fagsak.max_trans_id
                ,max(fagsak.annenforelderfagsak_id) annenforelderfagsak_id
                ,max(fam_fp_familiehendelse.til_aktoer_id) as aktoer_id, max(fam_fp_familiehendelse.kjoenn) as kjonn
                ,null as foedselsdato, null as sivilstand, null as statsborgerskap
          from fk_sensitiv.fam_fp_familiehendelse
          join fagsak
          on fam_fp_familiehendelse.fagsak_id = fagsak.fagsak_id
          where upper(fam_fp_familiehendelse.relasjon) = 'BARN'
          group by fagsak.fagsak_id, fagsak.max_trans_id
        ) person
        join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
        --join fk_person.fam_fp_person_67_vasket person_67_vasket
        on person_67_vasket.aktor_id = person.aktoer_id
        --on person_67_vasket.lk_person_id_kilde_num = person.aktoer_id
        group by person.person, person.fagsak_id, person.max_trans_id, person.aktoer_id
      ),
      barn as
      (
        select fagsak_id, listagg(fk_person1, ',') within group (order by fk_person1) as fk_person1_barn
        from fk_person1
        where person = 'BARN'
        group by fagsak_id
      ),
      mottaker as
      (
        select fk_person1.fagsak_id, fk_person1.behandlingstema
              ,fk_person1.max_trans_id, fk_person1.annenforelderfagsak_id, fk_person1.aktoer_id
              ,fk_person1.kjonn, fk_person1.fk_person1 as fk_person1_mottaker
              ,extract(year from fk_person1.foedselsdato) as mottaker_fodsels_aar
              ,extract(month from fk_person1.foedselsdato) as mottaker_fodsels_mnd
              ,fk_person1.sivilstand, fk_person1.statsborgerskap
              ,barn.fk_person1_barn
              ,termin.termindato, termin.foedselsdato, termin.antall_barn_termin, termin.antall_barn_foedsel
              ,termin.foedselsdato_adopsjon, termin.antall_barn_adopsjon
        from fk_person1
        left join barn
        on barn.fagsak_id = fk_person1.fagsak_id
        left join termin
        on fk_person1.fagsak_id = termin.fagsak_id
        where fk_person1.person = 'MOTTAKER'
      ),
      adopsjon as
      (
        select fam_fp_vilkaar.fagsak_id
              ,max(fam_fp_vilkaar.omsorgs_overtakelsesdato) as adopsjonsdato
              ,max(fam_fp_vilkaar.ektefelles_barn) as stebarnsadopsjon
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.fagsak_id = fam_fp_vilkaar.fagsak_id
        where fagsak.behandlingstema = 'FORP_ADOP'
        group by fam_fp_vilkaar.fagsak_id
      ),
      eos as
      (
       select a.trans_id
             ,case when upper(er_borger_av_eu_eos) = 'TRUE' then 'J'
                   when upper(er_borger_av_eu_eos) = 'FALSE' then 'N'
				  		    else null
              end	eos_sak
       from
       (select fam_fp_vilkaar.trans_id, max(fam_fp_vilkaar.er_borger_av_eu_eos) as er_borger_av_eu_eos
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.max_trans_id = fam_fp_vilkaar.trans_id
        and length(fam_fp_vilkaar.person_status) > 0
        group by fam_fp_vilkaar.trans_id
       ) a
      ),
      annenforelderfagsak as
      (
        select annenforelderfagsak.*, mottaker.fk_person1_mottaker as fk_person1_annen_part
        from
        (
          select fagsak_id, max_trans_id, max(annenforelderfagsak_id) as annenforelderfagsak_id
          from
          (
            select forelder1.fagsak_id, forelder1.max_trans_id
                  ,nvl(forelder1.annenforelderfagsak_id, forelder2.fagsak_id) as annenforelderfagsak_id
            from mottaker forelder1
            join mottaker forelder2
            on forelder1.fk_person1_barn = forelder2.fk_person1_barn
            and forelder1.fk_person1_mottaker != forelder2.fk_person1_mottaker
          )
          group by fagsak_id, max_trans_id
        ) annenforelderfagsak
        join mottaker
        on annenforelderfagsak.annenforelderfagsak_id = mottaker.fagsak_id
      ),
      tid as
      (
        select pk_dim_tid, dato, aar, halvaar, kvartal, aar_maaned
        from dt_kodeverk.dim_tid
        where dag_i_uke < 6   
        and dim_nivaa = 1
        and gyldig_flagg = 1
        and pk_dim_tid between p_tid_fom and p_tid_tom
        and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd')
      ),
      uttak as
      (
        select uttak.trans_id, uttak.trekkonto, uttak.uttak_arbeid_type, uttak.virksomhet, uttak.utbetalingsprosent
              ,uttak.gradering_innvilget, uttak.gradering, uttak.arbeidstidsprosent, uttak.samtidig_uttak
              ,uttak.periode_resultat_aarsak, uttak.fom as uttak_fom, uttak.tom as uttak_tom
              ,uttak.trekkdager
              ,fagsak.fagsak_id, fagsak.periode, fagsak.funksjonell_tid, fagsak.forste_vedtaksdato, fagsak.siste_vedtaksdato
              ,fagsak.max_vedtaksdato, fagsak.forste_soknadsdato, fagsak.soknadsdato
              ,fam_fp_trekkonto.pk_fam_fp_trekkonto
              ,aarsak_uttak.pk_fam_fp_periode_resultat_aarsak
              ,uttak.arbeidsforhold_id, uttak.graderingsdager
              ,fam_fp_uttak_fordelingsper.mors_aktivitet
         from fk_sensitiv.fam_fp_uttak_res_per_aktiv uttak
         join fagsak
         on fagsak.max_trans_id = uttak.trans_id
         left join dvh_fam_fp.fam_fp_trekkonto
         on upper(uttak.trekkonto) = fam_fp_trekkonto.trekkonto
         left join
         (select aarsak_uttak, max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
          from dvh_fam_fp.fam_fp_periode_resultat_aarsak
          group by aarsak_uttak
         ) aarsak_uttak
         on upper(uttak.periode_resultat_aarsak) = aarsak_uttak.aarsak_uttak
         left join fk_sensitiv.fam_fp_uttak_fordelingsper
         on fam_fp_uttak_fordelingsper.trans_id = uttak.trans_id
         and uttak.fom between fam_fp_uttak_fordelingsper.fom and fam_fp_uttak_fordelingsper.tom
         and upper(uttak.trekkonto) = upper(fam_fp_uttak_fordelingsper.periode_type)
         and length(fam_fp_uttak_fordelingsper.mors_aktivitet) > 1
         where uttak.utbetalingsprosent > 0
      ),
      stonadsdager_kvote as
      (
        select uttak.*, tid1.pk_dim_tid as fk_dim_tid_min_dato_kvote
              ,tid2.pk_dim_tid as fk_dim_tid_max_dato_kvote
        from
        (select fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
               ,sum(trekkdager) as stonadsdager_kvote, min(uttak_fom) as min_uttak_fom
               ,max(uttak_tom) as max_uttak_tom
         from
         (select fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
                ,max(trekkdager) as trekkdager
          from uttak
          group by fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
         ) a
         group by fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
        ) uttak
        join dt_kodeverk.dim_tid tid1
        on tid1.dim_nivaa = 1
        and tid1.dato = trunc(uttak.min_uttak_fom,'dd')
        join dt_kodeverk.dim_tid tid2
        on tid2.dim_nivaa = 1
        and tid2.dato = trunc(uttak.max_uttak_tom,'dd')
      ),
      uttak_dager AS 
      (
        select uttak.*
              ,tid.pk_dim_tid, tid.dato, tid.aar, tid.halvaar, tid.kvartal, tid.aar_maaned              
        from uttak
        join tid
        on tid.dato between uttak.uttak_fom and uttak.uttak_tom
      ),
      aleneomsorg as
      (
        select uttak.fagsak_id, uttak.uttak_fom
        from uttak
        join fk_sensitiv.fam_fp_dokumentasjonsperioder dok1
        on dok1.fagsak_id = uttak.fagsak_id
        and uttak.uttak_fom >= dok1.fom
        and dok1.dokumentasjon_type = 'ALENEOMSORG'
        left join fk_sensitiv.fam_fp_dokumentasjonsperioder dok2
        on dok1.fagsak_id = dok2.fagsak_id
        and uttak.uttak_fom >= dok2.fom
        and dok1.trans_id < dok2.trans_id
        and dok2.dokumentasjon_type = 'ANNEN_FORELDER_HAR_RETT'
        and dok2.fagsak_id is null
        group by uttak.fagsak_id, uttak.uttak_fom
      ),
      beregningsgrunnlag as
      (
        select fagsak_id, trans_id, virksomhetsnummer, max(status_og_andel_brutto) as status_og_andel_brutto
              ,max(status_og_andel_avkortet) as status_og_andel_avkortet
              ,fom as beregningsgrunnlag_fom, tom as beregningsgrunnlag_tom
              ,max(dekningsgrad) as dekningsgrad, max(dagsats) as dagsats, dagsats_bruker
              ,dagsats_arbeidsgiver
              ,dagsats_bruker+dagsats_arbeidsgiver dagsats_virksomhet
              ,max(status_og_andel_inntektskat) as status_og_andel_inntektskat
              ,aktivitet_status, max(brutto) as brutto_inntekt, max(avkortet) as avkortet_inntekt
              ,count(1) as antall_beregningsgrunnlag
        from fk_sensitiv.fam_fp_beregningsgrunnlag
        group by fagsak_id, trans_id, virksomhetsnummer, fom, tom, aktivitet_status, dagsats_bruker, dagsats_arbeidsgiver
      ),
      beregningsgrunnlag_detalj as
      (
        select uttak_dager.*
              ,stonadsdager_kvote.stonadsdager_kvote, stonadsdager_kvote.min_uttak_fom, stonadsdager_kvote.max_uttak_tom
              ,stonadsdager_kvote.fk_dim_tid_min_dato_kvote, stonadsdager_kvote.fk_dim_tid_max_dato_kvote
              ,bereg.status_og_andel_brutto, bereg.status_og_andel_avkortet, bereg.beregningsgrunnlag_fom
              ,bereg.dekningsgrad, bereg.beregningsgrunnlag_tom, bereg.dagsats, bereg.dagsats_bruker
              ,bereg.dagsats_arbeidsgiver
              ,bereg.dagsats_virksomhet, bereg.status_og_andel_inntektskat
              ,bereg.aktivitet_status, bereg.brutto_inntekt, bereg.avkortet_inntekt
              ,bereg.dagsats*uttak_dager.utbetalingsprosent/100 as dagsats_erst
              ,bereg.antall_beregningsgrunnlag
        from beregningsgrunnlag bereg
        join uttak_dager
        on uttak_dager.trans_id = bereg.trans_id
        and nvl(uttak_dager.virksomhet,'X') = nvl(bereg.virksomhetsnummer,'X')
        and bereg.beregningsgrunnlag_fom <= uttak_dager.dato
        and nvl(bereg.beregningsgrunnlag_tom,to_date('20991201','YYYYMMDD')) >= uttak_dager.dato
        left join stonadsdager_kvote
        on uttak_dager.trans_id = stonadsdager_kvote.trans_id
        and uttak_dager.trekkonto = stonadsdager_kvote.trekkonto
        and nvl(uttak_dager.virksomhet,'X') = nvl(stonadsdager_kvote.virksomhet,'X')
        and uttak_dager.uttak_arbeid_type = stonadsdager_kvote.uttak_arbeid_type
        join dvh_fam_fp.fam_fp_uttak_aktivitet_mapping uttak_mapping
        on uttak_dager.uttak_arbeid_type = uttak_mapping.uttak_arbeid
        and bereg.aktivitet_status = uttak_mapping.aktivitet_status
        where bereg.dagsats_bruker + bereg.dagsats_arbeidsgiver != 0
      ),
      beregningsgrunnlag_agg as
      (
        select a.*
              ,dager_erst*dagsats_virksomhet/dagsats*antall_beregningsgrunnlag tilfelle_erst
              ,dager_erst*round(utbetalingsprosent/100*dagsats_virksomhet) belop
              ,round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert
              ,case when periode_resultat_aarsak in (2004,2033) then 'N'	
			 	            when trekkonto in ('FEDREKVOTE','FELLESPERIODE','MØDREKVOTE') then 'J'
				            when trekkonto = 'FORELDREPENGER' then 'N'
			         end mor_rettighet
        from
        (
          select fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                ,aar, halvaar--, kvartal, aar_maaned
                ,uttak_fom, uttak_tom
                ,sum(dagsats_virksomhet/dagsats* case when ((upper(gradering_innvilget) ='TRUE' and upper(gradering)='TRUE') 
                                       or upper(samtidig_uttak)='TRUE') then (100-arbeidstidsprosent)/100
                               else 1.0
                          end
                     ) dager_erst2
                ,max(arbeidstidsprosent) as arbeidstidsprosent
                ,count(distinct pk_dim_tid) dager_erst
                ,
                 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
                 min(beregningsgrunnlag_fom) beregningsgrunnlag_fom, max(beregningsgrunnlag_tom) beregningsgrunnlag_tom
                ,dekningsgrad
                ,
                 --count(distinct pk_dim_tid)*
                 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
                 dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                ,virksomhet, periode_resultat_aarsak, dagsats, dagsats_erst
                , --dagsats_virksomhet,
                 utbetalingsprosent graderingsprosent, status_og_andel_inntektskat
                ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
                ,
                 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
                 utbetalingsprosent
                ,min(pk_dim_tid) pk_dim_tid_dato_utbet_fom, max(pk_dim_tid) pk_dim_tid_dato_utbet_tom
                ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                ,max(forste_soknadsdato) as forste_soknadsdato, max(soknadsdato) as soknadsdato
                ,samtidig_uttak, gradering, gradering_innvilget
                ,min_uttak_fom, max_uttak_tom
                ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                ,max(pk_fam_fp_trekkonto) as pk_fam_fp_trekkonto
                ,max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
                ,antall_beregningsgrunnlag, max(graderingsdager) as graderingsdager
                ,max(mors_aktivitet) as mors_aktivitet
          from beregningsgrunnlag_detalj
          group by fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                  ,aar, halvaar--, kvartal, aar_maaned
                  ,uttak_fom, uttak_tom, dekningsgrad
                  ,virksomhet, utbetalingsprosent, periode_resultat_aarsak
                  ,dagsats, dagsats_erst, dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                  ,utbetalingsprosent
                  ,status_og_andel_inntektskat, aktivitet_status, brutto_inntekt, avkortet_inntekt
                  ,status_og_andel_brutto, status_og_andel_avkortet
                  ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                  ,samtidig_uttak, gradering, gradering_innvilget, min_uttak_fom, max_uttak_tom
                  ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                  ,antall_beregningsgrunnlag
        ) a
      ),
      grunnlag as
      (
        select beregningsgrunnlag_agg.*, sysdate as lastet_dato
              ,mottaker.behandlingstema, mottaker.max_trans_id, mottaker.fk_person1_mottaker, mottaker.kjonn
              ,mottaker.fk_person1_barn
              ,mottaker.termindato, mottaker.foedselsdato, mottaker.antall_barn_termin
              ,mottaker.antall_barn_foedsel, mottaker.foedselsdato_adopsjon
              ,mottaker.antall_barn_adopsjon
              ,mottaker.mottaker_fodsels_aar, mottaker.mottaker_fodsels_mnd
              ,substr(p_tid_fom,1,4) - mottaker.mottaker_fodsels_aar as mottaker_alder
              ,mottaker.sivilstand, mottaker.statsborgerskap
              ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
              ,dim_person.fk_dim_sivilstatus
              ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr, dim_geografi.kommune_nr
              ,dim_geografi.kommune_navn, dim_geografi.bydel_nr, dim_geografi.bydel_navn
              ,annenforelderfagsak.annenforelderfagsak_id, annenforelderfagsak.fk_person1_annen_part
              ,fam_fp_uttak_fp_kontoer.max_dager max_stonadsdager_konto
              ,case when aleneomsorg.fagsak_id is not null then 'J' else NULL end as aleneomsorg
              ,case when behandlingstema = 'FORP_FODS' then '214'
                    when behandlingstema = 'FORP_ADOP' then '216'
               end as hovedkontonr
              ,case when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100<=50 then '1000'
               when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100>50 then '8020'
               --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
               when status_og_andel_inntektskat='JORDBRUKER' then '5210'
               when status_og_andel_inntektskat='SJØMANN' then '1300'
               when status_og_andel_inntektskat='SELVSTENDIG_NÆRINGSDRIVENDE' then '5010'
               when status_og_andel_inntektskat='DAGPENGER' then '1200'
               when status_og_andel_inntektskat='ARBEIDSTAKER_UTEN_FERIEPENGER' then '1000'
               when status_og_andel_inntektskat='FISKER' then '5300'
               when status_og_andel_inntektskat='DAGMAMMA' then '5110'
               when status_og_andel_inntektskat='FRILANSER' then '1100'
               end as underkontonr
              ,round(dagsats_arbeidsgiver/dagsats*100,0) as andel_av_refusjon
              ,case when rett_til_mødrekvote.trans_id is null then 'N' else 'J' end as rett_til_mødrekvote
              ,case when rett_til_fedrekvote.trans_id is null then 'N' else 'J' end as rett_til_fedrekvote
              ,flerbarnsdager.flerbarnsdager
              ,adopsjon.adopsjonsdato, adopsjon.stebarnsadopsjon
              ,eos.eos_sak
        from beregningsgrunnlag_agg
        left join mottaker
        on beregningsgrunnlag_agg.fagsak_id = mottaker.fagsak_id
        and beregningsgrunnlag_agg.trans_id = mottaker.max_trans_id
        left join annenforelderfagsak
        on beregningsgrunnlag_agg.fagsak_id = annenforelderfagsak.fagsak_id
        and beregningsgrunnlag_agg.trans_id = annenforelderfagsak.max_trans_id
        left join fk_sensitiv.fam_fp_uttak_fp_kontoer
        on beregningsgrunnlag_agg.fagsak_id = fam_fp_uttak_fp_kontoer.fagsak_id
        and mottaker.max_trans_id = fam_fp_uttak_fp_kontoer.trans_id
        --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
        and upper(replace(beregningsgrunnlag_agg.trekkonto,'_','')) = upper(replace(fam_fp_uttak_fp_kontoer.stoenadskontotype,' ',''))
        left join dt_person.dim_person
        on mottaker.fk_person1_mottaker = dim_person.fk_person1
        and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
        left join dt_kodeverk.dim_geografi
        on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        left join aleneomsorg
        on aleneomsorg.fagsak_id = beregningsgrunnlag_agg.fagsak_id
        and aleneomsorg.uttak_fom = beregningsgrunnlag_agg.uttak_fom
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'MØDREKVOTE'
         group by trans_id
        ) rett_til_mødrekvote
        on rett_til_mødrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FEDREKVOTE'
         group by trans_id
        ) rett_til_fedrekvote
        on rett_til_fedrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id, max(max_dager) as flerbarnsdager
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FLERBARNSDAGER'
         group by trans_id
        ) flerbarnsdager
        on flerbarnsdager.trans_id = beregningsgrunnlag_agg.trans_id
        left join adopsjon
        on beregningsgrunnlag_agg.fagsak_id = adopsjon.fagsak_id
        left join eos
        on beregningsgrunnlag_agg.trans_id = eos.trans_id
      )
      select /*+ PARALLEL(8) */ *
      --from uttak_dager
      from grunnlag
      --where fagsak_id in (1035184)
      ;
    v_tid_fom varchar2(8) := null;
    v_tid_tom varchar2(8) := null;
    v_commit number := 0;
    v_error_melding varchar2(1000) := null;
    v_dim_tid_antall number := 0;
    v_utbetalingsprosent_kalkulert number := 0;
  begin
    v_tid_fom := substr(p_in_vedtak_tom,1,4) || substr(p_in_vedtak_tom,5,6)-5 || '01';
    v_tid_tom := to_char(last_day(to_date(p_in_vedtak_tom,'yyyymm')),'yyyymmdd');
    --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!

    for rec_periode in cur_periode(p_in_rapport_dato, p_in_forskyvninger, v_tid_fom, v_tid_tom) loop
      v_dim_tid_antall := 0;
      v_utbetalingsprosent_kalkulert := 0;
      v_dim_tid_antall := dim_tid_antall(to_number(to_char(rec_periode.uttak_fom,'yyyymmdd'))
                                        ,to_number(to_char(rec_periode.uttak_tom,'yyyymmdd')));
      if v_dim_tid_antall != 0 then                                      
        v_utbetalingsprosent_kalkulert := round(rec_periode.trekkdager/v_dim_tid_antall*100,2);
      else
        v_utbetalingsprosent_kalkulert := 0;
      end if;
      begin
        insert into dvh_fam_fp.fam_fp_vedtak_utbetaling
        (fagsak_id, trans_id, behandlingstema, trekkonto, stonadsdager_kvote
        ,uttak_arbeid_type
        ,aar, halvaar, --AAR_MAANED,
         rapport_periode, uttak_fom, uttak_tom, dager_erst, beregningsgrunnlag_fom
        ,beregningsgrunnlag_tom, dekningsgrad, dagsats_bruker, dagsats_arbeidsgiver, virksomhet
        ,periode_resultat_aarsak, dagsats, graderingsprosent, status_og_andel_inntektskat
        ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
        ,utbetalingsprosent, fk_dim_tid_dato_utbet_fom
        ,fk_dim_tid_dato_utbet_tom, funksjonell_tid, forste_vedtaksdato, vedtaksdato, max_vedtaksdato, periode_type, tilfelle_erst
        ,belop, dagsats_redusert, lastet_dato, max_trans_id, fk_person1_mottaker, fk_person1_annen_part
        ,kjonn, fk_person1_barn, termindato, foedselsdato, antall_barn_termin, antall_barn_foedsel
        ,foedselsdato_adopsjon, antall_barn_adopsjon, annenforelderfagsak_id, max_stonadsdager_konto
        ,fk_dim_person, bosted_kommune_nr, fk_dim_geografi, bydel_kommune_nr, kommune_nr
        ,kommune_navn, bydel_nr, bydel_navn, aleneomsorg, hovedkontonr, underkontonr
        ,mottaker_fodsels_aar, mottaker_fodsels_mnd, mottaker_alder
        ,rett_til_fedrekvote, rett_til_modrekvote, dagsats_erst, trekkdager
        ,samtidig_uttak, gradering, gradering_innvilget, antall_dager_periode
        ,flerbarnsdager, utbetalingsprosent_kalkulert, min_uttak_fom, max_uttak_tom
        ,fk_fam_fp_trekkonto, fk_fam_fp_periode_resultat_aarsak
        ,sivilstatus, fk_dim_sivilstatus, antall_beregningsgrunnlag, graderingsdager
        ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
        ,adopsjonsdato, stebarnsadopsjon, eos_sak, mor_rettighet, statsborgerskap
        ,arbeidstidsprosent, mors_aktivitet, gyldig_flagg
        ,andel_av_refusjon, forste_soknadsdato, soknadsdato)
        values
        (rec_periode.fagsak_id, rec_periode.trans_id, rec_periode.behandlingstema, rec_periode.trekkonto
        ,rec_periode.stonadsdager_kvote
        ,rec_periode.uttak_arbeid_type, rec_periode.aar, rec_periode.halvaar--, AAR_MAANED
        ,p_in_rapport_dato, rec_periode.uttak_fom, rec_periode.uttak_tom
        ,rec_periode.dager_erst, rec_periode.beregningsgrunnlag_fom, rec_periode.beregningsgrunnlag_tom
        ,rec_periode.dekningsgrad, rec_periode.dagsats_bruker, rec_periode.dagsats_arbeidsgiver
        ,rec_periode.virksomhet, rec_periode.periode_resultat_aarsak, rec_periode.dagsats
        ,rec_periode.graderingsprosent, rec_periode.status_og_andel_inntektskat
        ,rec_periode.aktivitet_status, rec_periode.brutto_inntekt, rec_periode.avkortet_inntekt
        ,rec_periode.status_og_andel_brutto, rec_periode.status_og_andel_avkortet
        ,rec_periode.utbetalingsprosent, rec_periode.pk_dim_tid_dato_utbet_fom, rec_periode.pk_dim_tid_dato_utbet_tom
        ,rec_periode.funksjonell_tid, rec_periode.forste_vedtaksdato, rec_periode.siste_vedtaksdato, rec_periode.max_vedtaksdato, rec_periode.periode
        ,rec_periode.tilfelle_erst, rec_periode.belop, rec_periode.dagsats_redusert, rec_periode.lastet_dato
        ,rec_periode.max_trans_id, rec_periode.fk_person1_mottaker, rec_periode.fk_person1_annen_part
        ,rec_periode.kjonn, rec_periode.fk_person1_barn
        ,rec_periode.termindato, rec_periode.foedselsdato, rec_periode.antall_barn_termin
        ,rec_periode.antall_barn_foedsel, rec_periode.foedselsdato_adopsjon
        ,rec_periode.antall_barn_adopsjon, rec_periode.annenforelderfagsak_id, rec_periode.max_stonadsdager_konto
        ,rec_periode.pk_dim_person, rec_periode.bosted_kommune_nr, rec_periode.pk_dim_geografi
        ,rec_periode.bydel_kommune_nr, rec_periode.kommune_nr, rec_periode.kommune_navn
        ,rec_periode.bydel_nr, rec_periode.bydel_navn, rec_periode.aleneomsorg, rec_periode.hovedkontonr
        ,rec_periode.underkontonr
        ,rec_periode.mottaker_fodsels_aar, rec_periode.mottaker_fodsels_mnd, rec_periode.mottaker_alder
        ,rec_periode.rett_til_fedrekvote, rec_periode.rett_til_mødrekvote, rec_periode.dagsats_erst
        ,rec_periode.trekkdager, rec_periode.samtidig_uttak, rec_periode.gradering, rec_periode.gradering_innvilget
        ,v_dim_tid_antall, rec_periode.flerbarnsdager, v_utbetalingsprosent_kalkulert
        ,rec_periode.min_uttak_fom, rec_periode.max_uttak_tom, rec_periode.pk_fam_fp_trekkonto
        ,rec_periode.pk_fam_fp_periode_resultat_aarsak
        ,rec_periode.sivilstand, rec_periode.fk_dim_sivilstatus
        ,rec_periode.antall_beregningsgrunnlag, rec_periode.graderingsdager
        ,rec_periode.fk_dim_tid_min_dato_kvote, rec_periode.fk_dim_tid_max_dato_kvote
        ,rec_periode.adopsjonsdato, rec_periode.stebarnsadopsjon, rec_periode.eos_sak
        ,rec_periode.mor_rettighet, rec_periode.statsborgerskap
        ,rec_periode.arbeidstidsprosent, rec_periode.mors_aktivitet, p_in_gyldig_flagg
        ,rec_periode.andel_av_refusjon, rec_periode.forste_soknadsdato, rec_periode.soknadsdato);

        v_commit := v_commit + 1;
      exception
        when others then
          rollback;
          v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_periode.fagsak_id, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_HALVAAR:INSERT');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;

      if v_commit > 100000 then
        commit;
        v_commit := 0;
      end if;
   end loop;
   commit;
  exception
    when others then
      rollback;
      v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_HALVAAR');
      commit;
      p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
  end fam_fp_statistikk_halvaar;

  procedure fam_fp_statistikk_s(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'S'
                                     ,p_out_error out varchar2) as
    cursor cur_periode(p_rapport_dato in varchar2, p_forskyvninger in number, p_tid_fom in varchar2, p_tid_tom in varchar2) is
      with fagsak as
      (
        select fagsak_id, max(behandlingstema) as behandlingstema, max(fagsakannenforelder_id) as annenforelderfagsak_id
              ,max(trans_id) keep(dense_rank first order by funksjonell_tid desc) as max_trans_id
              ,max(soeknadsdato) keep(dense_rank first order by funksjonell_tid desc) as soknadsdato
              ,min(soeknadsdato) as forste_soknadsdato
              ,min(vedtaksdato) as forste_vedtaksdato
              ,max(funksjonell_tid) as funksjonell_tid, max(vedtaksdato) as siste_vedtaksdato
              ,p_in_periode_type as periode, last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger as max_vedtaksdato
        from fk_sensitiv.fam_fp_fagsak        
        where fam_fp_fagsak.funksjonell_tid <= last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger
        group by fagsak_id
      ),
      termin as
      (
        select fagsak_id, max(termindato) termindato, max(foedselsdato) foedselsdato
              ,max(antall_barn_termin) antall_barn_termin, max(antall_barn_foedsel) antall_barn_foedsel
              ,max(foedselsdato_adopsjon) foedselsdato_adopsjon, max(antall_barn_adopsjon) antall_barn_adopsjon
        from
        (
          select fam_fp_fagsak.fagsak_id, max(fodsel.termindato) termindato
                ,max(fodsel.foedselsdato) foedselsdato, max(fodsel.antall_barn_foedsel) antall_barn_foedsel
                ,max(fodsel.antall_barn_termin) antall_barn_termin
                ,max(adopsjon.foedselsdato_adopsjon) foedselsdato_adopsjon
                ,count(adopsjon.trans_id) antall_barn_adopsjon
          from fk_sensitiv.fam_fp_fagsak
          left join fk_sensitiv.fam_fp_fodseltermin fodsel
          on fodsel.fagsak_id = fam_fp_fagsak.fagsak_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_FODS'
          left join fk_sensitiv.fam_fp_fodseltermin adopsjon
          on adopsjon.fagsak_id = fam_fp_fagsak.fagsak_id
          and adopsjon.trans_id = fam_fp_fagsak.trans_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_ADOP'
          group by fam_fp_fagsak.fagsak_id, fam_fp_fagsak.trans_id
        )
        group by fagsak_id
      ),
      fk_person1 as
      (
        select person.person, person.fagsak_id, max(person.behandlingstema) as behandlingstema, person.max_trans_id
              ,max(person.annenforelderfagsak_id) as annenforelderfagsak_id
              ,person.aktoer_id, max(person.kjonn) as kjonn
              ,max(person_67_vasket.fk_person1) keep
                (dense_rank first order by person_67_vasket.gyldig_fra_dato desc) as fk_person1
              ,max(foedselsdato) as foedselsdato, max(sivilstand) as sivilstand
              ,max(statsborgerskap) as statsborgerskap
        from
        (
          select 'MOTTAKER' as person, fagsak.fagsak_id, fagsak.behandlingstema, fagsak.max_trans_id
                ,fagsak.annenforelderfagsak_id
                ,fam_fp_personopplysninger.aktoer_id, fam_fp_personopplysninger.kjonn
                ,fam_fp_personopplysninger.foedselsdato, fam_fp_personopplysninger.sivilstand
                ,fam_fp_personopplysninger.statsborgerskap
          from fk_sensitiv.fam_fp_personopplysninger
          join fagsak
          on fam_fp_personopplysninger.trans_id = fagsak.max_trans_id
          union all
          select 'BARN' as person, fagsak.fagsak_id, max(fagsak.behandlingstema) as behandlingstema, fagsak.max_trans_id
                ,max(fagsak.annenforelderfagsak_id) annenforelderfagsak_id
                ,max(fam_fp_familiehendelse.til_aktoer_id) as aktoer_id, max(fam_fp_familiehendelse.kjoenn) as kjonn
                ,null as foedselsdato, null as sivilstand, null as statsborgerskap
          from fk_sensitiv.fam_fp_familiehendelse
          join fagsak
          on fam_fp_familiehendelse.fagsak_id = fagsak.fagsak_id
          where upper(fam_fp_familiehendelse.relasjon) = 'BARN'
          group by fagsak.fagsak_id, fagsak.max_trans_id
        ) person
        join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
        on person_67_vasket.aktor_id = person.aktoer_id
        group by person.person, person.fagsak_id, person.max_trans_id, person.aktoer_id
      ),
      barn as
      (
        select fagsak_id, listagg(fk_person1, ',') within group (order by fk_person1) as fk_person1_barn
        from fk_person1
        where person = 'BARN'
        group by fagsak_id
      ),
      mottaker as
      (
        select fk_person1.fagsak_id, fk_person1.behandlingstema
              ,fk_person1.max_trans_id, fk_person1.annenforelderfagsak_id, fk_person1.aktoer_id
              ,fk_person1.kjonn, fk_person1.fk_person1 as fk_person1_mottaker
              ,extract(year from fk_person1.foedselsdato) as mottaker_fodsels_aar
              ,extract(month from fk_person1.foedselsdato) as mottaker_fodsels_mnd
              ,fk_person1.sivilstand, fk_person1.statsborgerskap
              ,barn.fk_person1_barn
              ,termin.termindato, termin.foedselsdato, termin.antall_barn_termin, termin.antall_barn_foedsel
              ,termin.foedselsdato_adopsjon, termin.antall_barn_adopsjon
        from fk_person1
        left join barn
        on barn.fagsak_id = fk_person1.fagsak_id
        left join termin
        on fk_person1.fagsak_id = termin.fagsak_id
        where fk_person1.person = 'MOTTAKER'
      ),
      adopsjon as
      (
        select fam_fp_vilkaar.fagsak_id
              ,max(fam_fp_vilkaar.omsorgs_overtakelsesdato) as adopsjonsdato
              ,max(fam_fp_vilkaar.ektefelles_barn) as stebarnsadopsjon
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.fagsak_id = fam_fp_vilkaar.fagsak_id
        where fagsak.behandlingstema = 'FORP_ADOP'
        group by fam_fp_vilkaar.fagsak_id
      ),
      eos as
      (
       select a.trans_id
             ,case when upper(er_borger_av_eu_eos) = 'TRUE' then 'J'
                   when upper(er_borger_av_eu_eos) = 'FALSE' then 'N'
				  		    else null
              end	eos_sak
       from
       (select fam_fp_vilkaar.trans_id, max(fam_fp_vilkaar.er_borger_av_eu_eos) as er_borger_av_eu_eos
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.max_trans_id = fam_fp_vilkaar.trans_id
        and length(fam_fp_vilkaar.person_status) > 0
        group by fam_fp_vilkaar.trans_id
       ) a
      ),
      annenforelderfagsak as
      (
        select annenforelderfagsak.*, mottaker.fk_person1_mottaker as fk_person1_annen_part
        from
        (
          select fagsak_id, max_trans_id, max(annenforelderfagsak_id) as annenforelderfagsak_id
          from
          (
            select forelder1.fagsak_id, forelder1.max_trans_id
                  ,nvl(forelder1.annenforelderfagsak_id, forelder2.fagsak_id) as annenforelderfagsak_id
            from mottaker forelder1
            join mottaker forelder2
            on forelder1.fk_person1_barn = forelder2.fk_person1_barn
            and forelder1.fk_person1_mottaker != forelder2.fk_person1_mottaker
          )
          group by fagsak_id, max_trans_id
        ) annenforelderfagsak
        join mottaker
        on annenforelderfagsak.annenforelderfagsak_id = mottaker.fagsak_id
      ),
      tid as
      (
        select pk_dim_tid, dato, aar, halvaar, kvartal, aar_maaned
        from dt_kodeverk.dim_tid
        where dag_i_uke < 6   
        and dim_nivaa = 1
        and gyldig_flagg = 1
        and pk_dim_tid between p_tid_fom and p_tid_tom
        and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd')
      ),
      uttak as
      (
        select uttak.trans_id, uttak.trekkonto, uttak.uttak_arbeid_type, uttak.virksomhet, uttak.utbetalingsprosent
              ,uttak.gradering_innvilget, uttak.gradering, uttak.arbeidstidsprosent, uttak.samtidig_uttak
              ,uttak.periode_resultat_aarsak, uttak.fom as uttak_fom, uttak.tom as uttak_tom
              ,uttak.trekkdager
              ,fagsak.fagsak_id, fagsak.periode, fagsak.funksjonell_tid, fagsak.forste_vedtaksdato, fagsak.siste_vedtaksdato
              ,fagsak.max_vedtaksdato, fagsak.forste_soknadsdato, fagsak.soknadsdato
              ,fam_fp_trekkonto.pk_fam_fp_trekkonto
              ,aarsak_uttak.pk_fam_fp_periode_resultat_aarsak
              ,uttak.arbeidsforhold_id, uttak.graderingsdager
              ,fam_fp_uttak_fordelingsper.mors_aktivitet
         from fk_sensitiv.fam_fp_uttak_res_per_aktiv uttak
         join fagsak
         on fagsak.max_trans_id = uttak.trans_id
         left join dvh_fam_fp.fam_fp_trekkonto
         on upper(uttak.trekkonto) = fam_fp_trekkonto.trekkonto
         left join
         (select aarsak_uttak, max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
          from dvh_fam_fp.fam_fp_periode_resultat_aarsak
          group by aarsak_uttak
         ) aarsak_uttak
         on upper(uttak.periode_resultat_aarsak) = aarsak_uttak.aarsak_uttak
         left join fk_sensitiv.fam_fp_uttak_fordelingsper
         on fam_fp_uttak_fordelingsper.trans_id = uttak.trans_id
         and uttak.fom between fam_fp_uttak_fordelingsper.fom and fam_fp_uttak_fordelingsper.tom
         and upper(uttak.trekkonto) = upper(fam_fp_uttak_fordelingsper.periode_type)
         and length(fam_fp_uttak_fordelingsper.mors_aktivitet) > 1
         where uttak.utbetalingsprosent > 0
      ),
      stonadsdager_kvote as
      (
        select uttak.*, tid1.pk_dim_tid as fk_dim_tid_min_dato_kvote
              ,tid2.pk_dim_tid as fk_dim_tid_max_dato_kvote
        from
        (select fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
               ,sum(trekkdager) as stonadsdager_kvote, min(uttak_fom) as min_uttak_fom
               ,max(uttak_tom) as max_uttak_tom
         from
         (select fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
                ,max(trekkdager) as trekkdager
          from uttak
          group by fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
         ) a
         group by fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
        ) uttak
        join dt_kodeverk.dim_tid tid1
        on tid1.dim_nivaa = 1
        and tid1.dato = trunc(uttak.min_uttak_fom,'dd')
        join dt_kodeverk.dim_tid tid2
        on tid2.dim_nivaa = 1
        and tid2.dato = trunc(uttak.max_uttak_tom,'dd')
      ),
      uttak_dager AS 
      (
        select uttak.*
              ,tid.pk_dim_tid, tid.dato, tid.aar, tid.halvaar, tid.kvartal, tid.aar_maaned              
        from uttak
        join tid
        on tid.dato between uttak.uttak_fom and uttak.uttak_tom
      ),
      aleneomsorg as
      (
        select uttak.fagsak_id, uttak.uttak_fom
        from uttak
        join fk_sensitiv.fam_fp_dokumentasjonsperioder dok1
        on dok1.fagsak_id = uttak.fagsak_id
        and uttak.uttak_fom >= dok1.fom
        and dok1.dokumentasjon_type = 'ALENEOMSORG'
        left join fk_sensitiv.fam_fp_dokumentasjonsperioder dok2
        on dok1.fagsak_id = dok2.fagsak_id
        and uttak.uttak_fom >= dok2.fom
        and dok1.trans_id < dok2.trans_id
        and dok2.dokumentasjon_type = 'ANNEN_FORELDER_HAR_RETT'
        and dok2.fagsak_id is null
        group by uttak.fagsak_id, uttak.uttak_fom
      ),
      beregningsgrunnlag as
      (
        select fagsak_id, trans_id, virksomhetsnummer, max(status_og_andel_brutto) as status_og_andel_brutto
              ,max(status_og_andel_avkortet) as status_og_andel_avkortet
              ,fom as beregningsgrunnlag_fom, tom as beregningsgrunnlag_tom
              ,max(dekningsgrad) as dekningsgrad, max(dagsats) as dagsats, dagsats_bruker
              ,dagsats_arbeidsgiver
              ,dagsats_bruker+dagsats_arbeidsgiver dagsats_virksomhet
              ,max(status_og_andel_inntektskat) as status_og_andel_inntektskat
              ,aktivitet_status, max(brutto) as brutto_inntekt, max(avkortet) as avkortet_inntekt
              ,count(1) as antall_beregningsgrunnlag
        from fk_sensitiv.fam_fp_beregningsgrunnlag
        group by fagsak_id, trans_id, virksomhetsnummer, fom, tom, aktivitet_status, dagsats_bruker, dagsats_arbeidsgiver
      ),
      beregningsgrunnlag_detalj as
      (
        select uttak_dager.*
              ,stonadsdager_kvote.stonadsdager_kvote, stonadsdager_kvote.min_uttak_fom, stonadsdager_kvote.max_uttak_tom
              ,stonadsdager_kvote.fk_dim_tid_min_dato_kvote, stonadsdager_kvote.fk_dim_tid_max_dato_kvote
              ,bereg.status_og_andel_brutto, bereg.status_og_andel_avkortet, bereg.beregningsgrunnlag_fom
              ,bereg.dekningsgrad, bereg.beregningsgrunnlag_tom, bereg.dagsats, bereg.dagsats_bruker
              ,bereg.dagsats_arbeidsgiver
              ,bereg.dagsats_virksomhet, bereg.status_og_andel_inntektskat
              ,bereg.aktivitet_status, bereg.brutto_inntekt, bereg.avkortet_inntekt
              ,bereg.dagsats*uttak_dager.utbetalingsprosent/100 as dagsats_erst
              ,bereg.antall_beregningsgrunnlag
        from beregningsgrunnlag bereg
        join uttak_dager
        on uttak_dager.trans_id = bereg.trans_id
        and nvl(uttak_dager.virksomhet,'X') = nvl(bereg.virksomhetsnummer,'X')
        and bereg.beregningsgrunnlag_fom <= uttak_dager.dato
        and nvl(bereg.beregningsgrunnlag_tom,to_date('20991201','YYYYMMDD')) >= uttak_dager.dato
        left join stonadsdager_kvote
        on uttak_dager.trans_id = stonadsdager_kvote.trans_id
        and uttak_dager.trekkonto = stonadsdager_kvote.trekkonto
        and nvl(uttak_dager.virksomhet,'X') = nvl(stonadsdager_kvote.virksomhet,'X')
        and uttak_dager.uttak_arbeid_type = stonadsdager_kvote.uttak_arbeid_type
        join dvh_fam_fp.fam_fp_uttak_aktivitet_mapping uttak_mapping
        on uttak_dager.uttak_arbeid_type = uttak_mapping.uttak_arbeid
        and bereg.aktivitet_status = uttak_mapping.aktivitet_status
        where bereg.dagsats_bruker + bereg.dagsats_arbeidsgiver != 0
      ),
      beregningsgrunnlag_agg as
      (
        select a.*
              ,dager_erst*dagsats_virksomhet/dagsats*antall_beregningsgrunnlag tilfelle_erst
              ,dager_erst*round(utbetalingsprosent/100*dagsats_virksomhet) belop
              ,round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert
              ,case when periode_resultat_aarsak in (2004,2033) then 'N'	
			 	            when trekkonto in ('FEDREKVOTE','FELLESPERIODE','MØDREKVOTE') then 'J'
				            when trekkonto = 'FORELDREPENGER' then 'N'
			         end mor_rettighet
        from
        (
          select fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                ,aar--, halvaar, kvartal, aar_maaned
                ,uttak_fom, uttak_tom
                ,sum(dagsats_virksomhet/dagsats* case when ((upper(gradering_innvilget) ='TRUE' and upper(gradering)='TRUE') 
                                       or upper(samtidig_uttak)='TRUE') then (100-arbeidstidsprosent)/100
                               else 1.0
                          end
                     ) dager_erst2
                ,max(arbeidstidsprosent) as arbeidstidsprosent
                ,count(distinct pk_dim_tid) dager_erst
                ,
                 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
                 min(beregningsgrunnlag_fom) beregningsgrunnlag_fom, max(beregningsgrunnlag_tom) beregningsgrunnlag_tom
                ,dekningsgrad
                ,
                 --count(distinct pk_dim_tid)*
                 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
                 dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                ,virksomhet, periode_resultat_aarsak, dagsats, dagsats_erst
                , --dagsats_virksomhet,
                 utbetalingsprosent graderingsprosent, status_og_andel_inntektskat
                ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
                ,
                 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
                 utbetalingsprosent
                ,min(pk_dim_tid) pk_dim_tid_dato_utbet_fom, max(pk_dim_tid) pk_dim_tid_dato_utbet_tom
                ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                ,max(forste_soknadsdato) as forste_soknadsdato, max(soknadsdato) as soknadsdato
                ,samtidig_uttak, gradering, gradering_innvilget
                ,min_uttak_fom, max_uttak_tom
                ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                ,max(pk_fam_fp_trekkonto) as pk_fam_fp_trekkonto
                ,max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
                ,antall_beregningsgrunnlag, max(graderingsdager) as graderingsdager
                ,max(mors_aktivitet) as mors_aktivitet
          from beregningsgrunnlag_detalj
          group by fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                  ,aar--, halvaar, kvartal, aar_maaned
                  ,uttak_fom, uttak_tom, dekningsgrad
                  ,virksomhet, utbetalingsprosent, periode_resultat_aarsak
                  ,dagsats, dagsats_erst, dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                  ,utbetalingsprosent
                  ,status_og_andel_inntektskat, aktivitet_status, brutto_inntekt, avkortet_inntekt
                  ,status_og_andel_brutto, status_og_andel_avkortet
                  ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                  ,samtidig_uttak, gradering, gradering_innvilget, min_uttak_fom, max_uttak_tom
                  ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                  ,antall_beregningsgrunnlag
        ) a
      ),
      grunnlag as
      (
        select beregningsgrunnlag_agg.*, sysdate as lastet_dato
              ,mottaker.behandlingstema, mottaker.max_trans_id, mottaker.fk_person1_mottaker, mottaker.kjonn
              ,mottaker.fk_person1_barn
              ,mottaker.termindato, mottaker.foedselsdato, mottaker.antall_barn_termin
              ,mottaker.antall_barn_foedsel, mottaker.foedselsdato_adopsjon
              ,mottaker.antall_barn_adopsjon
              ,mottaker.mottaker_fodsels_aar, mottaker.mottaker_fodsels_mnd
              ,substr(p_tid_fom,1,4) - mottaker.mottaker_fodsels_aar as mottaker_alder
              ,mottaker.sivilstand, mottaker.statsborgerskap
              ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
              ,dim_person.fk_dim_sivilstatus
              ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr, dim_geografi.kommune_nr
              ,dim_geografi.kommune_navn, dim_geografi.bydel_nr, dim_geografi.bydel_navn
              ,annenforelderfagsak.annenforelderfagsak_id, annenforelderfagsak.fk_person1_annen_part
              ,fam_fp_uttak_fp_kontoer.max_dager max_stonadsdager_konto
              ,case when aleneomsorg.fagsak_id is not null then 'J' else NULL end as aleneomsorg
              ,case when behandlingstema = 'FORP_FODS' then '214'
                    when behandlingstema = 'FORP_ADOP' then '216'
               end as hovedkontonr
              ,case when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100<=50 then '1000'
               when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100>50 then '8020'
               --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
               when status_og_andel_inntektskat='JORDBRUKER' then '5210'
               when status_og_andel_inntektskat='SJØMANN' then '1300'
               when status_og_andel_inntektskat='SELVSTENDIG_NÆRINGSDRIVENDE' then '5010'
               when status_og_andel_inntektskat='DAGPENGER' then '1200'
               when status_og_andel_inntektskat='ARBEIDSTAKER_UTEN_FERIEPENGER' then '1000'
               when status_og_andel_inntektskat='FISKER' then '5300'
               when status_og_andel_inntektskat='DAGMAMMA' then '5110'
               when status_og_andel_inntektskat='FRILANSER' then '1100'
               end as underkontonr
              ,round(dagsats_arbeidsgiver/dagsats*100,0) as andel_av_refusjon
              ,case when rett_til_mødrekvote.trans_id is null then 'N' else 'J' end as rett_til_mødrekvote
              ,case when rett_til_fedrekvote.trans_id is null then 'N' else 'J' end as rett_til_fedrekvote
              ,flerbarnsdager.flerbarnsdager
              ,adopsjon.adopsjonsdato, adopsjon.stebarnsadopsjon
              ,eos.eos_sak
        from beregningsgrunnlag_agg
        left join mottaker
        on beregningsgrunnlag_agg.fagsak_id = mottaker.fagsak_id
        and beregningsgrunnlag_agg.trans_id = mottaker.max_trans_id
        left join annenforelderfagsak
        on beregningsgrunnlag_agg.fagsak_id = annenforelderfagsak.fagsak_id
        and beregningsgrunnlag_agg.trans_id = annenforelderfagsak.max_trans_id
        left join fk_sensitiv.fam_fp_uttak_fp_kontoer
        on beregningsgrunnlag_agg.fagsak_id = fam_fp_uttak_fp_kontoer.fagsak_id
        and mottaker.max_trans_id = fam_fp_uttak_fp_kontoer.trans_id
        --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
        and upper(replace(beregningsgrunnlag_agg.trekkonto,'_','')) = upper(replace(fam_fp_uttak_fp_kontoer.stoenadskontotype,' ',''))
        left join dt_person.dim_person
        on mottaker.fk_person1_mottaker = dim_person.fk_person1
        and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
        left join dt_kodeverk.dim_geografi
        on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        left join aleneomsorg
        on aleneomsorg.fagsak_id = beregningsgrunnlag_agg.fagsak_id
        and aleneomsorg.uttak_fom = beregningsgrunnlag_agg.uttak_fom
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'MØDREKVOTE'
         group by trans_id
        ) rett_til_mødrekvote
        on rett_til_mødrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FEDREKVOTE'
         group by trans_id
        ) rett_til_fedrekvote
        on rett_til_fedrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id, max(max_dager) as flerbarnsdager
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FLERBARNSDAGER'
         group by trans_id
        ) flerbarnsdager
        on flerbarnsdager.trans_id = beregningsgrunnlag_agg.trans_id
        left join adopsjon
        on beregningsgrunnlag_agg.fagsak_id = adopsjon.fagsak_id
        left join eos
        on beregningsgrunnlag_agg.trans_id = eos.trans_id
      )
      select /*+ PARALLEL(8) */ *
      --from uttak_dager
      from grunnlag
      --where fagsak_id in (1035184)
      ;
    v_tid_fom varchar2(8) := null;
    v_tid_tom varchar2(8) := null;
    v_commit number := 0;
    v_error_melding varchar2(1000) := null;
    v_dim_tid_antall number := 0;
    v_utbetalingsprosent_kalkulert number := 0;
  begin
    v_tid_fom := substr(p_in_vedtak_tom,1,4) || '0101';
    v_tid_tom := to_char(last_day(to_date(p_in_vedtak_tom,'yyyymm')),'yyyymmdd');

    --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!

    for rec_periode in cur_periode(p_in_rapport_dato, p_in_forskyvninger, v_tid_fom, v_tid_tom) loop
      v_dim_tid_antall := 0;
      v_utbetalingsprosent_kalkulert := 0;
      v_dim_tid_antall := dim_tid_antall(to_number(to_char(rec_periode.uttak_fom,'yyyymmdd'))
                                        ,to_number(to_char(rec_periode.uttak_tom,'yyyymmdd')));
      if v_dim_tid_antall != 0 then                                      
        v_utbetalingsprosent_kalkulert := round(rec_periode.trekkdager/v_dim_tid_antall*100,2);
      else
        v_utbetalingsprosent_kalkulert := 0;
      end if;
      begin
        insert into dvh_fam_fp.fam_fp_vedtak_utbetaling
        (fagsak_id, trans_id, behandlingstema, trekkonto, stonadsdager_kvote
        ,uttak_arbeid_type
        ,aar--, halvaar, kvartal, aar_maaned
        ,rapport_periode, uttak_fom, uttak_tom, dager_erst, beregningsgrunnlag_fom
        ,beregningsgrunnlag_tom, dekningsgrad, dagsats_bruker, dagsats_arbeidsgiver, virksomhet
        ,periode_resultat_aarsak, dagsats, graderingsprosent, status_og_andel_inntektskat
        ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
        ,utbetalingsprosent, fk_dim_tid_dato_utbet_fom
        ,fk_dim_tid_dato_utbet_tom, funksjonell_tid, forste_vedtaksdato, vedtaksdato, max_vedtaksdato, periode_type, tilfelle_erst
        ,belop, dagsats_redusert, lastet_dato, max_trans_id, fk_person1_mottaker, fk_person1_annen_part
        ,kjonn, fk_person1_barn, termindato, foedselsdato, antall_barn_termin, antall_barn_foedsel
        ,foedselsdato_adopsjon, antall_barn_adopsjon, annenforelderfagsak_id, max_stonadsdager_konto
        ,fk_dim_person, bosted_kommune_nr, fk_dim_geografi, bydel_kommune_nr, kommune_nr
        ,kommune_navn, bydel_nr, bydel_navn, aleneomsorg, hovedkontonr, underkontonr
        ,mottaker_fodsels_aar, mottaker_fodsels_mnd, mottaker_alder
        ,rett_til_fedrekvote, rett_til_modrekvote, dagsats_erst, trekkdager
        ,samtidig_uttak, gradering, gradering_innvilget, antall_dager_periode
        ,flerbarnsdager, utbetalingsprosent_kalkulert, min_uttak_fom, max_uttak_tom
        ,fk_fam_fp_trekkonto, fk_fam_fp_periode_resultat_aarsak
        ,sivilstatus, fk_dim_sivilstatus, antall_beregningsgrunnlag, graderingsdager
        ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
        ,adopsjonsdato, stebarnsadopsjon, eos_sak, mor_rettighet, statsborgerskap
        ,arbeidstidsprosent, mors_aktivitet, gyldig_flagg
        ,andel_av_refusjon, forste_soknadsdato, soknadsdato)
        values
        (rec_periode.fagsak_id, rec_periode.trans_id, rec_periode.behandlingstema, rec_periode.trekkonto
        ,rec_periode.stonadsdager_kvote
        ,rec_periode.uttak_arbeid_type, rec_periode.aar--, rec_periode.halvaar, rec_periode.kvartal, rec_periode.aar_maaned
        ,p_in_rapport_dato, rec_periode.uttak_fom, rec_periode.uttak_tom
        ,rec_periode.dager_erst, rec_periode.beregningsgrunnlag_fom, rec_periode.beregningsgrunnlag_tom
        ,rec_periode.dekningsgrad, rec_periode.dagsats_bruker, rec_periode.dagsats_arbeidsgiver
        ,rec_periode.virksomhet, rec_periode.periode_resultat_aarsak, rec_periode.dagsats
        ,rec_periode.graderingsprosent, rec_periode.status_og_andel_inntektskat
        ,rec_periode.aktivitet_status, rec_periode.brutto_inntekt, rec_periode.avkortet_inntekt
        ,rec_periode.status_og_andel_brutto, rec_periode.status_og_andel_avkortet
        ,rec_periode.utbetalingsprosent, rec_periode.pk_dim_tid_dato_utbet_fom, rec_periode.pk_dim_tid_dato_utbet_tom
        ,rec_periode.funksjonell_tid, rec_periode.forste_vedtaksdato, rec_periode.siste_vedtaksdato, rec_periode.max_vedtaksdato, rec_periode.periode
        ,rec_periode.tilfelle_erst, rec_periode.belop, rec_periode.dagsats_redusert, rec_periode.lastet_dato
        ,rec_periode.max_trans_id, rec_periode.fk_person1_mottaker, rec_periode.fk_person1_annen_part
        ,rec_periode.kjonn, rec_periode.fk_person1_barn
        ,rec_periode.termindato, rec_periode.foedselsdato, rec_periode.antall_barn_termin
        ,rec_periode.antall_barn_foedsel, rec_periode.foedselsdato_adopsjon
        ,rec_periode.antall_barn_adopsjon, rec_periode.annenforelderfagsak_id, rec_periode.max_stonadsdager_konto
        ,rec_periode.pk_dim_person, rec_periode.bosted_kommune_nr, rec_periode.pk_dim_geografi
        ,rec_periode.bydel_kommune_nr, rec_periode.kommune_nr, rec_periode.kommune_navn
        ,rec_periode.bydel_nr, rec_periode.bydel_navn, rec_periode.aleneomsorg, rec_periode.hovedkontonr
        ,rec_periode.underkontonr
        ,rec_periode.mottaker_fodsels_aar, rec_periode.mottaker_fodsels_mnd, rec_periode.mottaker_alder
        ,rec_periode.rett_til_fedrekvote, rec_periode.rett_til_mødrekvote, rec_periode.dagsats_erst
        ,rec_periode.trekkdager, rec_periode.samtidig_uttak, rec_periode.gradering, rec_periode.gradering_innvilget
        ,v_dim_tid_antall, rec_periode.flerbarnsdager, v_utbetalingsprosent_kalkulert
        ,rec_periode.min_uttak_fom, rec_periode.max_uttak_tom, rec_periode.pk_fam_fp_trekkonto
        ,rec_periode.pk_fam_fp_periode_resultat_aarsak
        ,rec_periode.sivilstand, rec_periode.fk_dim_sivilstatus
        ,rec_periode.antall_beregningsgrunnlag, rec_periode.graderingsdager
        ,rec_periode.fk_dim_tid_min_dato_kvote, rec_periode.fk_dim_tid_max_dato_kvote
        ,rec_periode.adopsjonsdato, rec_periode.stebarnsadopsjon, rec_periode.eos_sak
        ,rec_periode.mor_rettighet, rec_periode.statsborgerskap
        ,rec_periode.arbeidstidsprosent, rec_periode.mors_aktivitet, p_in_gyldig_flagg
        ,rec_periode.andel_av_refusjon, rec_periode.forste_soknadsdato, rec_periode.soknadsdato);

        v_commit := v_commit + 1;
      exception
        when others then
          rollback;
          v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_periode.fagsak_id, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_S:INSERT');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;

      if v_commit > 100000 then
        commit;
        v_commit := 0;
      end if;
   end loop;
   commit;
  exception
    when others then
      rollback;
      v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_S');
      commit;
      p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
  end fam_fp_statistikk_s;

  procedure fam_fp_statistikk_aar(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'A'
                                     ,p_out_error out varchar2) as
    cursor cur_periode(p_rapport_dato in varchar2, p_forskyvninger in number, p_tid_fom in varchar2, p_tid_tom in varchar2) is
      with fagsak as
      (
        select fagsak_id, max(behandlingstema) as behandlingstema, max(fagsakannenforelder_id) as annenforelderfagsak_id
              ,max(trans_id) keep(dense_rank first order by funksjonell_tid desc) as max_trans_id
              ,max(soeknadsdato) keep(dense_rank first order by funksjonell_tid desc) as soknadsdato
              ,min(soeknadsdato) as forste_soknadsdato
              ,min(vedtaksdato) as forste_vedtaksdato
              ,max(funksjonell_tid) as funksjonell_tid, max(vedtaksdato) as siste_vedtaksdato
              ,p_in_periode_type as periode, last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger as max_vedtaksdato
        from fk_sensitiv.fam_fp_fagsak        
        where fam_fp_fagsak.funksjonell_tid <= last_day(to_date(p_rapport_dato,'yyyymm')) + p_forskyvninger
        group by fagsak_id
      ),
      termin as
      (
        select fagsak_id, max(termindato) termindato, max(foedselsdato) foedselsdato
              ,max(antall_barn_termin) antall_barn_termin, max(antall_barn_foedsel) antall_barn_foedsel
              ,max(foedselsdato_adopsjon) foedselsdato_adopsjon, max(antall_barn_adopsjon) antall_barn_adopsjon
        from
        (
          select fam_fp_fagsak.fagsak_id, max(fodsel.termindato) termindato
                ,max(fodsel.foedselsdato) foedselsdato, max(fodsel.antall_barn_foedsel) antall_barn_foedsel
                ,max(fodsel.antall_barn_termin) antall_barn_termin
                ,max(adopsjon.foedselsdato_adopsjon) foedselsdato_adopsjon
                ,count(adopsjon.trans_id) antall_barn_adopsjon
          from fk_sensitiv.fam_fp_fagsak
          left join fk_sensitiv.fam_fp_fodseltermin fodsel
          on fodsel.fagsak_id = fam_fp_fagsak.fagsak_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_FODS'
          left join fk_sensitiv.fam_fp_fodseltermin adopsjon
          on adopsjon.fagsak_id = fam_fp_fagsak.fagsak_id
          and adopsjon.trans_id = fam_fp_fagsak.trans_id
          and upper(fam_fp_fagsak.behandlingstema) = 'FORP_ADOP'
          group by fam_fp_fagsak.fagsak_id, fam_fp_fagsak.trans_id
        )
        group by fagsak_id
      ),
      fk_person1 as
      (
        select person.person, person.fagsak_id, max(person.behandlingstema) as behandlingstema, person.max_trans_id
              ,max(person.annenforelderfagsak_id) as annenforelderfagsak_id
              ,person.aktoer_id, max(person.kjonn) as kjonn
              ,max(person_67_vasket.fk_person1) keep
                (dense_rank first order by person_67_vasket.gyldig_fra_dato desc) as fk_person1
              ,max(foedselsdato) as foedselsdato, max(sivilstand) as sivilstand
              ,max(statsborgerskap) as statsborgerskap
        from
        (
          select 'MOTTAKER' as person, fagsak.fagsak_id, fagsak.behandlingstema, fagsak.max_trans_id
                ,fagsak.annenforelderfagsak_id
                ,fam_fp_personopplysninger.aktoer_id, fam_fp_personopplysninger.kjonn
                ,fam_fp_personopplysninger.foedselsdato, fam_fp_personopplysninger.sivilstand
                ,fam_fp_personopplysninger.statsborgerskap
          from fk_sensitiv.fam_fp_personopplysninger
          join fagsak
          on fam_fp_personopplysninger.trans_id = fagsak.max_trans_id
          union all
          select 'BARN' as person, fagsak.fagsak_id, max(fagsak.behandlingstema) as behandlingstema, fagsak.max_trans_id
                ,max(fagsak.annenforelderfagsak_id) annenforelderfagsak_id
                ,max(fam_fp_familiehendelse.til_aktoer_id) as aktoer_id, max(fam_fp_familiehendelse.kjoenn) as kjonn
                ,null as foedselsdato, null as sivilstand, null as statsborgerskap
          from fk_sensitiv.fam_fp_familiehendelse
          join fagsak
          on fam_fp_familiehendelse.fagsak_id = fagsak.fagsak_id
          where upper(fam_fp_familiehendelse.relasjon) = 'BARN'
          group by fagsak.fagsak_id, fagsak.max_trans_id
        ) person
        join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
        on person_67_vasket.aktor_id = person.aktoer_id
        group by person.person, person.fagsak_id, person.max_trans_id, person.aktoer_id
      ),
      barn as
      (
        select fagsak_id, listagg(fk_person1, ',') within group (order by fk_person1) as fk_person1_barn
        from fk_person1
        where person = 'BARN'
        group by fagsak_id
      ),
      mottaker as
      (
        select fk_person1.fagsak_id, fk_person1.behandlingstema
              ,fk_person1.max_trans_id, fk_person1.annenforelderfagsak_id, fk_person1.aktoer_id
              ,fk_person1.kjonn, fk_person1.fk_person1 as fk_person1_mottaker
              ,extract(year from fk_person1.foedselsdato) as mottaker_fodsels_aar
              ,extract(month from fk_person1.foedselsdato) as mottaker_fodsels_mnd
              ,fk_person1.sivilstand, fk_person1.statsborgerskap
              ,barn.fk_person1_barn
              ,termin.termindato, termin.foedselsdato, termin.antall_barn_termin, termin.antall_barn_foedsel
              ,termin.foedselsdato_adopsjon, termin.antall_barn_adopsjon
        from fk_person1
        left join barn
        on barn.fagsak_id = fk_person1.fagsak_id
        left join termin
        on fk_person1.fagsak_id = termin.fagsak_id
        where fk_person1.person = 'MOTTAKER'
      ),
      adopsjon as
      (
        select fam_fp_vilkaar.fagsak_id
              ,max(fam_fp_vilkaar.omsorgs_overtakelsesdato) as adopsjonsdato
              ,max(fam_fp_vilkaar.ektefelles_barn) as stebarnsadopsjon
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.fagsak_id = fam_fp_vilkaar.fagsak_id
        where fagsak.behandlingstema = 'FORP_ADOP'
        group by fam_fp_vilkaar.fagsak_id
      ),
      eos as
      (
       select a.trans_id
             ,case when upper(er_borger_av_eu_eos) = 'TRUE' then 'J'
                   when upper(er_borger_av_eu_eos) = 'FALSE' then 'N'
				  		    else null
              end	eos_sak
       from
       (select fam_fp_vilkaar.trans_id, max(fam_fp_vilkaar.er_borger_av_eu_eos) as er_borger_av_eu_eos
        from fagsak
        join fk_sensitiv.fam_fp_vilkaar
        on fagsak.max_trans_id = fam_fp_vilkaar.trans_id
        and length(fam_fp_vilkaar.person_status) > 0
        group by fam_fp_vilkaar.trans_id
       ) a
      ),
      annenforelderfagsak as
      (
        select annenforelderfagsak.*, mottaker.fk_person1_mottaker as fk_person1_annen_part
        from
        (
          select fagsak_id, max_trans_id, max(annenforelderfagsak_id) as annenforelderfagsak_id
          from
          (
            select forelder1.fagsak_id, forelder1.max_trans_id
                  ,nvl(forelder1.annenforelderfagsak_id, forelder2.fagsak_id) as annenforelderfagsak_id
            from mottaker forelder1
            join mottaker forelder2
            on forelder1.fk_person1_barn = forelder2.fk_person1_barn
            and forelder1.fk_person1_mottaker != forelder2.fk_person1_mottaker
          )
          group by fagsak_id, max_trans_id
        ) annenforelderfagsak
        join mottaker
        on annenforelderfagsak.annenforelderfagsak_id = mottaker.fagsak_id
      ),
      tid as
      (
        select pk_dim_tid, dato, aar, halvaar, kvartal, aar_maaned
        from dt_kodeverk.dim_tid
        where dag_i_uke < 6   
        and dim_nivaa = 1
        and gyldig_flagg = 1
        and pk_dim_tid between p_tid_fom and p_tid_tom
        and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd')
      ),
      uttak as
      (
        select uttak.trans_id, uttak.trekkonto, uttak.uttak_arbeid_type, uttak.virksomhet, uttak.utbetalingsprosent
              ,uttak.gradering_innvilget, uttak.gradering, uttak.arbeidstidsprosent, uttak.samtidig_uttak
              ,uttak.periode_resultat_aarsak, uttak.fom as uttak_fom, uttak.tom as uttak_tom
              ,uttak.trekkdager
              ,fagsak.fagsak_id, fagsak.periode, fagsak.funksjonell_tid, fagsak.forste_vedtaksdato, fagsak.siste_vedtaksdato
              ,fagsak.max_vedtaksdato, fagsak.forste_soknadsdato, fagsak.soknadsdato
              ,fam_fp_trekkonto.pk_fam_fp_trekkonto
              ,aarsak_uttak.pk_fam_fp_periode_resultat_aarsak
              ,uttak.arbeidsforhold_id, uttak.graderingsdager
              ,fam_fp_uttak_fordelingsper.mors_aktivitet
         from fk_sensitiv.fam_fp_uttak_res_per_aktiv uttak
         join fagsak
         on fagsak.max_trans_id = uttak.trans_id
         left join dvh_fam_fp.fam_fp_trekkonto
         on upper(uttak.trekkonto) = fam_fp_trekkonto.trekkonto
         left join
         (select aarsak_uttak, max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
          from dvh_fam_fp.fam_fp_periode_resultat_aarsak
          group by aarsak_uttak
         ) aarsak_uttak
         on upper(uttak.periode_resultat_aarsak) = aarsak_uttak.aarsak_uttak
         left join fk_sensitiv.fam_fp_uttak_fordelingsper
         on fam_fp_uttak_fordelingsper.trans_id = uttak.trans_id
         and uttak.fom between fam_fp_uttak_fordelingsper.fom and fam_fp_uttak_fordelingsper.tom
         and upper(uttak.trekkonto) = upper(fam_fp_uttak_fordelingsper.periode_type)
         and length(fam_fp_uttak_fordelingsper.mors_aktivitet) > 1
         where uttak.utbetalingsprosent > 0
      ),
      stonadsdager_kvote as
      (
        select uttak.*, tid1.pk_dim_tid as fk_dim_tid_min_dato_kvote
              ,tid2.pk_dim_tid as fk_dim_tid_max_dato_kvote
        from
        (select fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
               ,sum(trekkdager) as stonadsdager_kvote, min(uttak_fom) as min_uttak_fom
               ,max(uttak_tom) as max_uttak_tom
         from
         (select fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
                ,max(trekkdager) as trekkdager
          from uttak
          group by fagsak_id, trans_id, uttak_fom, uttak_tom, trekkonto, virksomhet, uttak_arbeid_type
         ) a
         group by fagsak_id, trans_id, trekkonto, virksomhet, uttak_arbeid_type
        ) uttak
        join dt_kodeverk.dim_tid tid1
        on tid1.dim_nivaa = 1
        and tid1.dato = trunc(uttak.min_uttak_fom,'dd')
        join dt_kodeverk.dim_tid tid2
        on tid2.dim_nivaa = 1
        and tid2.dato = trunc(uttak.max_uttak_tom,'dd')
      ),
      uttak_dager AS 
      (
        select uttak.*
              ,tid.pk_dim_tid, tid.dato, tid.aar, tid.halvaar, tid.kvartal, tid.aar_maaned              
        from uttak
        join tid
        on tid.dato between uttak.uttak_fom and uttak.uttak_tom
      ),
      aleneomsorg as
      (
        select uttak.fagsak_id, uttak.uttak_fom
        from uttak
        join fk_sensitiv.fam_fp_dokumentasjonsperioder dok1
        on dok1.fagsak_id = uttak.fagsak_id
        and uttak.uttak_fom >= dok1.fom
        and dok1.dokumentasjon_type = 'ALENEOMSORG'
        left join fk_sensitiv.fam_fp_dokumentasjonsperioder dok2
        on dok1.fagsak_id = dok2.fagsak_id
        and uttak.uttak_fom >= dok2.fom
        and dok1.trans_id < dok2.trans_id
        and dok2.dokumentasjon_type = 'ANNEN_FORELDER_HAR_RETT'
        and dok2.fagsak_id is null
        group by uttak.fagsak_id, uttak.uttak_fom
      ),
      beregningsgrunnlag as
      (
        select fagsak_id, trans_id, virksomhetsnummer, max(status_og_andel_brutto) as status_og_andel_brutto
              ,max(status_og_andel_avkortet) as status_og_andel_avkortet
              ,fom as beregningsgrunnlag_fom, tom as beregningsgrunnlag_tom
              ,max(dekningsgrad) as dekningsgrad, max(dagsats) as dagsats, dagsats_bruker
              ,dagsats_arbeidsgiver
              ,dagsats_bruker+dagsats_arbeidsgiver dagsats_virksomhet
              ,max(status_og_andel_inntektskat) as status_og_andel_inntektskat
              ,aktivitet_status, max(brutto) as brutto_inntekt, max(avkortet) as avkortet_inntekt
              ,count(1) as antall_beregningsgrunnlag
        from fk_sensitiv.fam_fp_beregningsgrunnlag
        group by fagsak_id, trans_id, virksomhetsnummer, fom, tom, aktivitet_status, dagsats_bruker, dagsats_arbeidsgiver
      ),
      beregningsgrunnlag_detalj as
      (
        select uttak_dager.*
              ,stonadsdager_kvote.stonadsdager_kvote, stonadsdager_kvote.min_uttak_fom, stonadsdager_kvote.max_uttak_tom
              ,stonadsdager_kvote.fk_dim_tid_min_dato_kvote, stonadsdager_kvote.fk_dim_tid_max_dato_kvote
              ,bereg.status_og_andel_brutto, bereg.status_og_andel_avkortet, bereg.beregningsgrunnlag_fom
              ,bereg.dekningsgrad, bereg.beregningsgrunnlag_tom, bereg.dagsats, bereg.dagsats_bruker
              ,bereg.dagsats_arbeidsgiver
              ,bereg.dagsats_virksomhet, bereg.status_og_andel_inntektskat
              ,bereg.aktivitet_status, bereg.brutto_inntekt, bereg.avkortet_inntekt
              ,bereg.dagsats*uttak_dager.utbetalingsprosent/100 as dagsats_erst
              ,bereg.antall_beregningsgrunnlag
        from beregningsgrunnlag bereg
        join uttak_dager
        on uttak_dager.trans_id = bereg.trans_id
        and nvl(uttak_dager.virksomhet,'X') = nvl(bereg.virksomhetsnummer,'X')
        and bereg.beregningsgrunnlag_fom <= uttak_dager.dato
        and nvl(bereg.beregningsgrunnlag_tom,to_date('20991201','YYYYMMDD')) >= uttak_dager.dato
        left join stonadsdager_kvote
        on uttak_dager.trans_id = stonadsdager_kvote.trans_id
        and uttak_dager.trekkonto = stonadsdager_kvote.trekkonto
        and nvl(uttak_dager.virksomhet,'X') = nvl(stonadsdager_kvote.virksomhet,'X')
        and uttak_dager.uttak_arbeid_type = stonadsdager_kvote.uttak_arbeid_type
        join dvh_fam_fp.fam_fp_uttak_aktivitet_mapping uttak_mapping
        on uttak_dager.uttak_arbeid_type = uttak_mapping.uttak_arbeid
        and bereg.aktivitet_status = uttak_mapping.aktivitet_status
        where bereg.dagsats_bruker + bereg.dagsats_arbeidsgiver != 0
      ),
      beregningsgrunnlag_agg as
      (
        select a.*
              ,dager_erst*dagsats_virksomhet/dagsats*antall_beregningsgrunnlag tilfelle_erst
              ,dager_erst*round(utbetalingsprosent/100*dagsats_virksomhet) belop
              ,round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert
              ,case when periode_resultat_aarsak in (2004,2033) then 'N'	
			 	            when trekkonto in ('FEDREKVOTE','FELLESPERIODE','MØDREKVOTE') then 'J'
				            when trekkonto = 'FORELDREPENGER' then 'N'
			         end mor_rettighet
        from
        (
          select fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                ,aar--, halvaar--, kvartal, aar_maaned
                ,uttak_fom, uttak_tom
                ,sum(dagsats_virksomhet/dagsats* case when ((upper(gradering_innvilget) ='TRUE' and upper(gradering)='TRUE') 
                                       or upper(samtidig_uttak)='TRUE') then (100-arbeidstidsprosent)/100
                               else 1.0
                          end
                     ) dager_erst2
                ,max(arbeidstidsprosent) as arbeidstidsprosent
                ,count(distinct pk_dim_tid) dager_erst
                ,
                 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
                 min(beregningsgrunnlag_fom) beregningsgrunnlag_fom, max(beregningsgrunnlag_tom) beregningsgrunnlag_tom
                ,dekningsgrad
                ,
                 --count(distinct pk_dim_tid)*
                 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
                 dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                ,virksomhet, periode_resultat_aarsak, dagsats, dagsats_erst
                , --dagsats_virksomhet,
                 utbetalingsprosent graderingsprosent, status_og_andel_inntektskat
                ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
                ,
                 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
                 utbetalingsprosent
                ,min(pk_dim_tid) pk_dim_tid_dato_utbet_fom, max(pk_dim_tid) pk_dim_tid_dato_utbet_tom
                ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                ,max(forste_soknadsdato) as forste_soknadsdato, max(soknadsdato) as soknadsdato
                ,samtidig_uttak, gradering, gradering_innvilget
                ,min_uttak_fom, max_uttak_tom
                ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                ,max(pk_fam_fp_trekkonto) as pk_fam_fp_trekkonto
                ,max(pk_fam_fp_periode_resultat_aarsak) as pk_fam_fp_periode_resultat_aarsak
                ,antall_beregningsgrunnlag, max(graderingsdager) as graderingsdager
                ,max(mors_aktivitet) as mors_aktivitet
          from beregningsgrunnlag_detalj
          group by fagsak_id, trans_id, trekkonto, trekkdager, stonadsdager_kvote, uttak_arbeid_type
                  ,aar--, halvaar--, kvartal, aar_maaned
                  ,uttak_fom, uttak_tom, dekningsgrad
                  ,virksomhet, utbetalingsprosent, periode_resultat_aarsak
                  ,dagsats, dagsats_erst, dagsats_bruker, dagsats_arbeidsgiver, dagsats_virksomhet
                  ,utbetalingsprosent
                  ,status_og_andel_inntektskat, aktivitet_status, brutto_inntekt, avkortet_inntekt
                  ,status_og_andel_brutto, status_og_andel_avkortet
                  ,funksjonell_tid, forste_vedtaksdato, siste_vedtaksdato, max_vedtaksdato, periode
                  ,samtidig_uttak, gradering, gradering_innvilget, min_uttak_fom, max_uttak_tom
                  ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
                  ,antall_beregningsgrunnlag
        ) a
      ),
      grunnlag as
      (
        select beregningsgrunnlag_agg.*, sysdate as lastet_dato
              ,mottaker.behandlingstema, mottaker.max_trans_id, mottaker.fk_person1_mottaker, mottaker.kjonn
              ,mottaker.fk_person1_barn
              ,mottaker.termindato, mottaker.foedselsdato, mottaker.antall_barn_termin
              ,mottaker.antall_barn_foedsel, mottaker.foedselsdato_adopsjon
              ,mottaker.antall_barn_adopsjon
              ,mottaker.mottaker_fodsels_aar, mottaker.mottaker_fodsels_mnd
              ,substr(p_tid_fom,1,4) - mottaker.mottaker_fodsels_aar as mottaker_alder
              ,mottaker.sivilstand, mottaker.statsborgerskap
              ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
              ,dim_person.fk_dim_sivilstatus
              ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr, dim_geografi.kommune_nr
              ,dim_geografi.kommune_navn, dim_geografi.bydel_nr, dim_geografi.bydel_navn
              ,annenforelderfagsak.annenforelderfagsak_id, annenforelderfagsak.fk_person1_annen_part
              ,fam_fp_uttak_fp_kontoer.max_dager max_stonadsdager_konto
              ,case when aleneomsorg.fagsak_id is not null then 'J' else NULL end as aleneomsorg
              ,case when behandlingstema = 'FORP_FODS' then '214'
                    when behandlingstema = 'FORP_ADOP' then '216'
               end as hovedkontonr
              ,case when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100<=50 then '1000'
               when status_og_andel_inntektskat='ARBEIDSTAKER' 
                         and dagsats_arbeidsgiver/dagsats*100>50 then '8020'
               --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
               when status_og_andel_inntektskat='JORDBRUKER' then '5210'
               when status_og_andel_inntektskat='SJØMANN' then '1300'
               when status_og_andel_inntektskat='SELVSTENDIG_NÆRINGSDRIVENDE' then '5010'
               when status_og_andel_inntektskat='DAGPENGER' then '1200'
               when status_og_andel_inntektskat='ARBEIDSTAKER_UTEN_FERIEPENGER' then '1000'
               when status_og_andel_inntektskat='FISKER' then '5300'
               when status_og_andel_inntektskat='DAGMAMMA' then '5110'
               when status_og_andel_inntektskat='FRILANSER' then '1100'
               end as underkontonr
              ,round(dagsats_arbeidsgiver/dagsats*100,0) as andel_av_refusjon
              ,case when rett_til_mødrekvote.trans_id is null then 'N' else 'J' end as rett_til_mødrekvote
              ,case when rett_til_fedrekvote.trans_id is null then 'N' else 'J' end as rett_til_fedrekvote
              ,flerbarnsdager.flerbarnsdager
              ,adopsjon.adopsjonsdato, adopsjon.stebarnsadopsjon
              ,eos.eos_sak
        from beregningsgrunnlag_agg
        left join mottaker
        on beregningsgrunnlag_agg.fagsak_id = mottaker.fagsak_id
        and beregningsgrunnlag_agg.trans_id = mottaker.max_trans_id
        left join annenforelderfagsak
        on beregningsgrunnlag_agg.fagsak_id = annenforelderfagsak.fagsak_id
        and beregningsgrunnlag_agg.trans_id = annenforelderfagsak.max_trans_id
        left join fk_sensitiv.fam_fp_uttak_fp_kontoer
        on beregningsgrunnlag_agg.fagsak_id = fam_fp_uttak_fp_kontoer.fagsak_id
        and mottaker.max_trans_id = fam_fp_uttak_fp_kontoer.trans_id
        --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
        and upper(replace(beregningsgrunnlag_agg.trekkonto,'_','')) = upper(replace(fam_fp_uttak_fp_kontoer.stoenadskontotype,' ',''))
        left join dt_person.dim_person
        on mottaker.fk_person1_mottaker = dim_person.fk_person1
        and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
        left join dt_kodeverk.dim_geografi
        on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        left join aleneomsorg
        on aleneomsorg.fagsak_id = beregningsgrunnlag_agg.fagsak_id
        and aleneomsorg.uttak_fom = beregningsgrunnlag_agg.uttak_fom
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'MØDREKVOTE'
         group by trans_id
        ) rett_til_mødrekvote
        on rett_til_mødrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FEDREKVOTE'
         group by trans_id
        ) rett_til_fedrekvote
        on rett_til_fedrekvote.trans_id = beregningsgrunnlag_agg.trans_id
        left join
        (select trans_id, max(max_dager) as flerbarnsdager
         from fk_sensitiv.fam_fp_uttak_fp_kontoer
         where upper(stoenadskontotype) = 'FLERBARNSDAGER'
         group by trans_id
        ) flerbarnsdager
        on flerbarnsdager.trans_id = beregningsgrunnlag_agg.trans_id
        left join adopsjon
        on beregningsgrunnlag_agg.fagsak_id = adopsjon.fagsak_id
        left join eos
        on beregningsgrunnlag_agg.trans_id = eos.trans_id
      )
      select /*+ PARALLEL(8) */ *
      --from uttak_dager
      from grunnlag
      --where fagsak_id in (1035184)
      ;
    v_tid_fom varchar2(8) := null;
    v_tid_tom varchar2(8) := null;
    v_commit number := 0;
    v_error_melding varchar2(1000) := null;
    v_dim_tid_antall number := 0;
    v_utbetalingsprosent_kalkulert number := 0;
  begin
    v_tid_fom := substr(p_in_vedtak_tom,1,4) || '0101';
    v_tid_tom := substr(p_in_vedtak_tom,1,4) || '1231';
    --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!

    for rec_periode in cur_periode(p_in_rapport_dato, p_in_forskyvninger, v_tid_fom, v_tid_tom) loop
      v_dim_tid_antall := 0;
      v_utbetalingsprosent_kalkulert := 0;
      v_dim_tid_antall := dim_tid_antall(to_number(to_char(rec_periode.uttak_fom,'yyyymmdd'))
                                        ,to_number(to_char(rec_periode.uttak_tom,'yyyymmdd')));
      if v_dim_tid_antall != 0 then                                      
        v_utbetalingsprosent_kalkulert := round(rec_periode.trekkdager/v_dim_tid_antall*100,2);
      else
        v_utbetalingsprosent_kalkulert := 0;
      end if;
      --dbms_output.put_line(v_dim_tid_antall);
      begin
        insert into dvh_fam_fp.fam_fp_vedtak_utbetaling
        (fagsak_id, trans_id, behandlingstema, trekkonto, stonadsdager_kvote
        ,uttak_arbeid_type
        ,aar--, halvaar, --AAR_MAANED,
        ,rapport_periode, uttak_fom, uttak_tom, dager_erst, beregningsgrunnlag_fom
        ,beregningsgrunnlag_tom, dekningsgrad, dagsats_bruker, dagsats_arbeidsgiver, virksomhet
        ,periode_resultat_aarsak, dagsats, graderingsprosent, status_og_andel_inntektskat
        ,aktivitet_status, brutto_inntekt, avkortet_inntekt, status_og_andel_brutto, status_og_andel_avkortet
        ,utbetalingsprosent, fk_dim_tid_dato_utbet_fom
        ,fk_dim_tid_dato_utbet_tom, funksjonell_tid, forste_vedtaksdato, vedtaksdato, max_vedtaksdato, periode_type, tilfelle_erst
        ,belop, dagsats_redusert, lastet_dato, max_trans_id, fk_person1_mottaker, fk_person1_annen_part
        ,kjonn, fk_person1_barn, termindato, foedselsdato, antall_barn_termin, antall_barn_foedsel
        ,foedselsdato_adopsjon, antall_barn_adopsjon, annenforelderfagsak_id, max_stonadsdager_konto
        ,fk_dim_person, bosted_kommune_nr, fk_dim_geografi, bydel_kommune_nr, kommune_nr
        ,kommune_navn, bydel_nr, bydel_navn, aleneomsorg, hovedkontonr, underkontonr
        ,mottaker_fodsels_aar, mottaker_fodsels_mnd, mottaker_alder
        ,rett_til_fedrekvote, rett_til_modrekvote, dagsats_erst, trekkdager
        ,samtidig_uttak, gradering, gradering_innvilget, antall_dager_periode
        ,flerbarnsdager, utbetalingsprosent_kalkulert, min_uttak_fom, max_uttak_tom
        ,fk_fam_fp_trekkonto, fk_fam_fp_periode_resultat_aarsak
        ,sivilstatus, fk_dim_sivilstatus, antall_beregningsgrunnlag, graderingsdager
        ,fk_dim_tid_min_dato_kvote, fk_dim_tid_max_dato_kvote
        ,adopsjonsdato, stebarnsadopsjon, eos_sak, mor_rettighet, statsborgerskap
        ,arbeidstidsprosent, mors_aktivitet, gyldig_flagg
        ,andel_av_refusjon, forste_soknadsdato, soknadsdato)
        values
        (rec_periode.fagsak_id, rec_periode.trans_id, rec_periode.behandlingstema, rec_periode.trekkonto
        ,rec_periode.stonadsdager_kvote
        ,rec_periode.uttak_arbeid_type, rec_periode.aar--, rec_periode.halvaar--, AAR_MAANED
        ,p_in_rapport_dato, rec_periode.uttak_fom, rec_periode.uttak_tom
        ,rec_periode.dager_erst, rec_periode.beregningsgrunnlag_fom, rec_periode.beregningsgrunnlag_tom
        ,rec_periode.dekningsgrad, rec_periode.dagsats_bruker, rec_periode.dagsats_arbeidsgiver
        ,rec_periode.virksomhet, rec_periode.periode_resultat_aarsak, rec_periode.dagsats
        ,rec_periode.graderingsprosent, rec_periode.status_og_andel_inntektskat
        ,rec_periode.aktivitet_status, rec_periode.brutto_inntekt, rec_periode.avkortet_inntekt
        ,rec_periode.status_og_andel_brutto, rec_periode.status_og_andel_avkortet
        ,rec_periode.utbetalingsprosent, rec_periode.pk_dim_tid_dato_utbet_fom, rec_periode.pk_dim_tid_dato_utbet_tom
        ,rec_periode.funksjonell_tid, rec_periode.forste_vedtaksdato, rec_periode.siste_vedtaksdato, rec_periode.max_vedtaksdato, rec_periode.periode
        ,rec_periode.tilfelle_erst, rec_periode.belop, rec_periode.dagsats_redusert, rec_periode.lastet_dato
        ,rec_periode.max_trans_id, rec_periode.fk_person1_mottaker, rec_periode.fk_person1_annen_part
        ,rec_periode.kjonn, rec_periode.fk_person1_barn
        ,rec_periode.termindato, rec_periode.foedselsdato, rec_periode.antall_barn_termin
        ,rec_periode.antall_barn_foedsel, rec_periode.foedselsdato_adopsjon
        ,rec_periode.antall_barn_adopsjon, rec_periode.annenforelderfagsak_id, rec_periode.max_stonadsdager_konto
        ,rec_periode.pk_dim_person, rec_periode.bosted_kommune_nr, rec_periode.pk_dim_geografi
        ,rec_periode.bydel_kommune_nr, rec_periode.kommune_nr, rec_periode.kommune_navn
        ,rec_periode.bydel_nr, rec_periode.bydel_navn, rec_periode.aleneomsorg, rec_periode.hovedkontonr
        ,rec_periode.underkontonr
        ,rec_periode.mottaker_fodsels_aar, rec_periode.mottaker_fodsels_mnd, rec_periode.mottaker_alder
        ,rec_periode.rett_til_fedrekvote, rec_periode.rett_til_mødrekvote, rec_periode.dagsats_erst
        ,rec_periode.trekkdager, rec_periode.samtidig_uttak, rec_periode.gradering, rec_periode.gradering_innvilget
        ,v_dim_tid_antall, rec_periode.flerbarnsdager, v_utbetalingsprosent_kalkulert
        ,rec_periode.min_uttak_fom, rec_periode.max_uttak_tom, rec_periode.pk_fam_fp_trekkonto
        ,rec_periode.pk_fam_fp_periode_resultat_aarsak
        ,rec_periode.sivilstand, rec_periode.fk_dim_sivilstatus
        ,rec_periode.antall_beregningsgrunnlag, rec_periode.graderingsdager
        ,rec_periode.fk_dim_tid_min_dato_kvote, rec_periode.fk_dim_tid_max_dato_kvote
        ,rec_periode.adopsjonsdato, rec_periode.stebarnsadopsjon, rec_periode.eos_sak
        ,rec_periode.mor_rettighet, rec_periode.statsborgerskap
        ,rec_periode.arbeidstidsprosent, rec_periode.mors_aktivitet, p_in_gyldig_flagg
        ,rec_periode.andel_av_refusjon, rec_periode.forste_soknadsdato, rec_periode.soknadsdato);

        v_commit := v_commit + 1;
      exception
        when others then
          rollback;
          v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_periode.fagsak_id, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_AAR:INSERT');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;

      if v_commit > 100000 then
        commit;
        v_commit := 0;
      end if;
   end loop;
   commit;
  exception
    when others then
      rollback;
      v_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, v_error_melding, sysdate, 'FAM_FP_STATISTIKK_AAR');
      commit;
      p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
  end fam_fp_statistikk_aar; 

END FAM_FP;