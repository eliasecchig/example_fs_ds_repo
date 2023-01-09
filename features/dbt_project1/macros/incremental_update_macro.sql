{% macro incremental_date_filter(
    source_col_name,
    target_col_name,
    relation = "this"
)
%}
{#
Macro that filters out already processed data in an incremental table.
There is the possibility to extend the process to recover previous days already processed
see here https://www.kolibrigames.com/blog/making-data-mitigation-easy/

Args:
source col name (datetime / date) : Date Column from data being run
target_col_name (datetime / date) : Date Column from data already run
relation( relation). : Relation to which this filter must be applied to. Default actual table
Returns:
AND statement for a WHERE clause that performs a filter on incremental models.
Note:
Please remember to provide actual string of column names to be used in
the macro code, for example where the columns names being used are
"timestamp_column", the code should be like this
{{ incremental filter('timestamp_column', 'timestamp_column') }}
#}
{% if is_incremental() %}
AND (
    DATE({{source_col_name}}) > (
    SELECT MAX(DATE({{target_col_name}}))
    FROM {% if relation == "this" %} {{ this }} {% else %} {{ relation }} {% endif %}
    )
)
{% endif %}
{% endmacro %}