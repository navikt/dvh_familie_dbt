with kafka_ny_losning as (
  select * from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

final as (
select
k.melding.person.personIdent as person_ident,
k.melding.person.rolle as rolle
k.melding.person.bostedland as bostedland,
k.melding.person.delingsprosentYtelse as delingsprosent_ytelse
  from kafka_ny_losning k
)


select * from final
