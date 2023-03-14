{% macro incremental_date_filter(
    source_column_name,
    target_column_name,
    relation = "this"
)
%}
{#
Macro that filters out already processed data in an incremental table.

Args:
source_column_name [Date/Datetime/Timestamp] : column of the data being modelled that will be used to find the delta
target_column_name (datetime / date) :  column from the target table that will be used to find the delta
relation( string ) : Table/View on which the filter will be applied. It defaults to the actual table
#}
{% if is_incremental() %}
AND (
    DATE({{ source_column_name }}) > (
    SELECT MAX(DATE({{ target_column_name }}))
    FROM {% if relation == "this" %} {{ this }} {% else %} {{ relation }} {% endif %}
    )
)
{% endif %}
{% endmacro %}