{% macro date_convert(date, timezone='Europe/Belgrade') %}
    ({{ column_name }} / 100)::numeric(16, {{ scale }})

{% set now = modules.datetime.datetime.now() %}
{% set three_days_ago_iso = (now - modules.datetime.timedelta(3)).isoformat() %}

{% set dt = modules.datetime.datetime(2002, 10, 27, 6, 0, 0) %}
{% set dt_local = modules.pytz.timezone('US/Eastern').localize(dt) %}
{{ dt_local }}

{% endmacro %}