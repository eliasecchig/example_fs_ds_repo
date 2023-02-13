function default__rolling_window(aggregation, columnsToAggregate, partition, timestampColumn, frameSecondsStart, frameSecondsEnd) {
  return `${aggregation}(${columnsToAggregate}) OVER (PARTITION BY ${partition} ORDER BY UNIX_SECONDS(${timestampColumn}) RANGE BETWEEN ${frameSecondsStart} PRECEDING AND ${frameSecondsEnd})`;
}

function cast_aggregation_start(aggregation) {
  if (aggregation.toLowerCase() !== 'count') {
    return `CAST(`;
  }
  else{
    return ''
  }
}

function cast_aggregation_end(aggregation) {
  if (aggregation.toLowerCase() !== 'count') {
    return ` AS FLOAT64)`;
  }
  else{
    return ''
  }
}

function default__compute_rolling_windows({ aggregations, columnsToAggregate, partition, timestampColumn, lookbackWindowsStart, excludeCurrentRow = true}) {
  const seconds_per_unit = { "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800 };
  let partitionColumns = partition;
  let partitionColumnsName = partition.replace(',', '_').toLowerCase();
  let frameSecondsEnd = "CURRENT ROW";
  let lookbackWindowEnd = "0s";
  if (excludeCurrentRow) {
    frameSecondsEnd = "1 PRECEDING";
    lookbackWindowEnd = "1s";
  }
  let result = "";
  for (let columnToAggregate of columnsToAggregate) {
    for (let aggregation of aggregations) {
      for (let lookbackWindowStart of lookbackWindowsStart) {
        let frameSecondsStart = (parseInt(lookbackWindowStart.slice(0, -1)) * (seconds_per_unit[lookbackWindowStart[lookbackWindowStart.length - 1]]));
        let castAggregationStart = cast_aggregation_start(aggregation);
        let rollingWindow = default__rolling_window(
          aggregation=aggregation,
          columnToAggregate=columnToAggregate,
          partitionColumns=partitionColumns,
          timestampColumn=timestampColumn,
          frameSecondsStart=frameSecondsStart,
          frameSecondsEnd=frameSecondsEnd,
        );
        let castAggregationEnd = cast_aggregation_end(aggregation);
        let column = `${aggregation.toLowerCase()}_${columnToAggregate.toLowerCase()}_by_${partitionColumnsName}_${lookbackWindowStart}_${lookbackWindowEnd}`;
        result += `${castAggregationStart}${rollingWindow}${castAggregationEnd} AS ${column},\n`;

      }
    }
  }
  return result;
}
module.exports = { default__compute_rolling_windows };
