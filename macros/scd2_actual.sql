{% macro scd2_actual(base_table) %}

SELECT *
FROM {{ base_table }}
WHERE is_current = true;

{% endmacro %}
