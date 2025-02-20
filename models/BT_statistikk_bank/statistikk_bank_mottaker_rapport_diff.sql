{{
    config(
        materialized='view'
    )
}}

--I alt: Sammenligning av summen
select kjonn, alder_gruppe_besk
      ,max(antall_2015) antall_2015
      ,max(antall_2015_publisert) antall_2015_publisert
      ,round((max(antall_2015)-max(antall_2015_publisert))/max(antall_2015_publisert)*100,2) antall_2015_diff_prosent

      ,max(antall_2016) antall_2016
      ,max(antall_2016_publisert) antall_2016_publisert
      ,round((max(antall_2016)-max(antall_2016_publisert))/max(antall_2016_publisert)*100,2) antall_2016_diff_prosent

      ,max(antall_2017) antall_2017
      ,max(antall_2017_publisert) antall_2017_publisert
      ,round((max(antall_2017)-max(antall_2017_publisert))/max(antall_2017_publisert)*100,2) antall_2017_diff_prosent

      ,max(antall_2018) antall_2018
      ,max(antall_2018_publisert) antall_2018_publisert
      ,round((max(antall_2018)-max(antall_2018_publisert))/max(antall_2018_publisert)*100,2) antall_2018_diff_prosent

      ,max(antall_2019) antall_2019
      ,max(antall_2019_publisert) antall_2019_publisert
      ,round((max(antall_2019)-max(antall_2019_publisert))/max(antall_2019_publisert)*100,2) antall_2019_diff_prosent

      ,max(antall_2020) antall_2020
      ,max(antall_2020_publisert) antall_2020_publisert
      ,round((max(antall_2020)-max(antall_2020_publisert))/max(antall_2020_publisert)*100,2) antall_2020_diff_prosent

      ,max(antall_2021) antall_2021
      ,max(antall_2021_publisert) antall_2021_publisert
      ,round((max(antall_2021)-max(antall_2021_publisert))/max(antall_2021_publisert)*100,2) antall_2021_diff_prosent

      ,max(antall_2022) antall_2022
      ,max(antall_2022_publisert) antall_2022_publisert
      ,round((max(antall_2022)-max(antall_2022_publisert))/max(antall_2022_publisert)*100,2) antall_2022_diff_prosent

      ,max(antall_2023) antall_2023
      ,max(antall_2023_publisert) antall_2023_publisert
      ,round((max(antall_2023)-max(antall_2023_publisert))/max(antall_2023_publisert)*100,2) antall_2023_diff_prosent

      ,max(antall_2024) antall_2024
      ,max(antall_2024_publisert) antall_2024_publisert
      ,round((max(antall_2024)-max(antall_2024_publisert))/max(antall_2024_publisert)*100,2) antall_2024_diff_prosent
from
(
  select mottaker.aar, mottaker.alder_gruppe_besk, mottaker.kjonn

        ,case when mottaker.aar = 2015 then mottaker.antall end antall_2015
        ,publisert.aar_2015 antall_2015_publisert

        ,case when mottaker.aar = 2016 then mottaker.antall end antall_2016
        ,publisert.aar_2016 antall_2016_publisert

        ,case when mottaker.aar = 2017 then mottaker.antall end antall_2017
        ,publisert.aar_2017 antall_2017_publisert

        ,case when mottaker.aar = 2018 then mottaker.antall end antall_2018
        ,publisert.aar_2018 antall_2018_publisert

        ,case when mottaker.aar = 2019 then mottaker.antall end antall_2019
        ,publisert.aar_2019 antall_2019_publisert

        ,case when mottaker.aar = 2020 then mottaker.antall end antall_2020
        ,publisert.aar_2020 antall_2020_publisert

        ,case when mottaker.aar = 2021 then mottaker.antall end antall_2021
        ,publisert.aar_2021 antall_2021_publisert

        ,case when mottaker.aar = 2022 then mottaker.antall end antall_2022
        ,publisert.aar_2022 antall_2022_publisert

        ,case when mottaker.aar = 2023 then mottaker.antall end antall_2023
        ,publisert.aar_2023 antall_2023_publisert

        ,case when mottaker.aar = 2024 then mottaker.antall end antall_2024
        ,publisert.aar_2024 antall_2024_publisert

  from {{ ref('agg_bt_statistikk_bank_mottaker') }} mottaker

  left join {{ source('statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_rapport_nav') }} publisert
  on decode(mottaker.kjonn, 'ALT', 'I alt', 'K', 'Kvinner', 'M', 'Menn') = publisert.kjonn
  and decode(mottaker.alder_gruppe_besk, 'ALT', 'I alt', mottaker.alder_gruppe_besk) = publisert.felt_besk
)
group by alder_gruppe_besk, kjonn
order by kjonn, alder_gruppe_besk