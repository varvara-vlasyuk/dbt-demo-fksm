{% macro remove_dashes(field_name) %}
    REPLACE({{ field_name }}, '-', '')
{% endmacro %}