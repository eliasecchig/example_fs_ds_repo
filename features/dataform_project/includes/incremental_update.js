function incremental_date_filter(source_col_name, target_col_name, relation) {
    // Macro that filters out already processed data in an incremental table.

    // Args:
    // source_column_name [Date/Datetime/Timestamp] : column of the data being modelled that will be used to find the delta
    // target_col_name (datetime / date) :  column from the target table that will be used to find the delta
    // relation( string ) : Table/View on which the filter will be applied. It defaults to the actual table
    return `AND (
        DATE(${source_col_name}) > (
        SELECT MAX(DATE(${target_col_name}))
        FROM ${relation}
        )
    )`;
}
module.exports = { incremental_date_filter };
