{% macro generate_sequences() %}

    {% if execute %}

    {% set models = graph.nodes.values() | selectattr('resource_type', 'eq', 'model') %}
    {# parse through the graph object, find all models with the meta surrogate key config #}
    {% set sk_models = [] %}
    {% for model in models %}
        {% if model.config.meta.surrogate_key %}
          {% do sk_models.append(model) %}
        {% endif %}
    {% endfor %}

    {% endif %}

    {% for model in sk_models %}

        {% if flags.FULL_REFRESH or model.config.materialized == 'table' %}
        {# regenerate sequences if necessary #}

        DECLARE
          found NUMBER;
        BEGIN
          -- try to find sequence in data dictionary
          SELECT count(1)
          INTO found
          FROM user_sequences
          WHERE sequence_name = 'TEST_SEQ'; --{{ model.name }}_seq;

          IF found = 0 THEN
            EXECUTE IMMEDIATE 'create sequence TEST_SEQ'; --||{{ model.name }}_seq;
          ELSE
            EXECUTE IMMEDIATE 'DROP SEQUENCE TEST_SEQ'; --||{{ model.name }}_seq;
            EXECUTE IMMEDIATE 'create sequence TEST_SEQ'; --||{{ model.name }}_seq;
          END IF;
        END;

        {#create sequence {{ model.schema }}.{{ model.name }}_seq#}

        {% else %}
        {# create only if not exists for incremental models #}

        create sequence {{ model.schema }}.{{ model.name }}_seq

        {% endif %}

    {% endfor %}

{% endmacro %}