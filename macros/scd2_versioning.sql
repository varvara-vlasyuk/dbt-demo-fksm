{% macro scd2_versioning(source_data, base_table, unique_key, versioning_columns) %}

WITH ranked_data AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY {{ unique_key }} ORDER BY {{ versioning_columns|join(', ') }} DESC) as row_number,
           CURRENT_DATE() as effective_date,
           '9999-12-31' as expiration_date,
           true as is_current
    FROM {{ source_data }}
),

updates AS (
    SELECT *
    FROM ranked_data
    WHERE row_number = 1
)

{% if is_incremental() %}

, expired AS (
    SELECT base.*,
           CURRENT_DATE() as expiration_date,
           false as is_current
    FROM {{ this }} base
    INNER JOIN updates ON base.{{ unique_key }} = updates.{{ unique_key }}
    WHERE base.is_current = true
)

{% endif %}

SELECT * FROM updates

{% if is_incremental() %}

    UNION ALL

    SELECT * FROM {{ this }}
    WHERE NOT EXISTS (
        SELECT 1
        FROM updates
        WHERE {{ this }}.{{ unique_key }} = updates.{{ unique_key }}
    )

    UNION ALL

    SELECT * FROM expired

{% endif %}

{% endmacro %}
