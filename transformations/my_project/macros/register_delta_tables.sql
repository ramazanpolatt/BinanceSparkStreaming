{% macro register_delta_tables() %}

    {% set check_table_query %}
        SELECT table_name
        FROM delta.information_schema.tables
        WHERE table_schema = 'default' AND table_name = 'binance'
    {% endset %}

    {% set results = run_query(check_table_query) %}

    {% if execute %}
        {% if results|length == 0 %}
            {{ log("Registering Delta table 'binance'...", info=True) }}
            {% set register_query %}
                CREATE SCHEMA IF NOT EXISTS delta.default;
                CALL delta.system.register_table(
                    schema_name => 'default',
                    table_name => 'binance',
                    table_location => 's3://spark-crypto/binance/'
                );
            {% endset %}
            {% do run_query(register_query) %}
        {% else %}
            {{ log("Delta table 'binance' is already registered.", info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}