// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Initial data ingestion

// COMMAND ----------

// input data params (substitute with your values)
spark.conf.set("fs.azure.account.auth.type.<acc-name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<acc-name>.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.id.<acc-name>.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.<acc-name>.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<acc-name>.dfs.core.windows.net", "")

// access key for results storage 
spark.conf.set("fs.azure.account.key.stsparsqlwesteurope.dfs.core.windows.net", "")


// COMMAND ----------

// ingest expedia data into Azure storage
spark.read
  .format("avro")
  .load("abfss://m07sparksql@<acc-name>.dfs.core.windows.net/expedia")
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparsqlwesteurope.dfs.core.windows.net/expedia")

// ingest hotel-weather data into Azure storage
spark.read
  .format("parquet")
  .load("abfss://m07sparksql@<acc-name>.dfs.core.windows.net/hotel-weather")
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather")


// COMMAND ----------

// create database
spark.sql("CREATE DATABASE IF NOT EXISTS M07_SPARKSQL_AZURE_DB")
spark.sql("USE M07_SPARKSQL_AZURE_DB")

// create expedia delta table
val expedia_ddl_query = """CREATE TABLE IF NOT EXISTS M07_SPARKSQL_AZURE_DB.EXPEDIA 
                   USING DELTA
                   LOCATION 'abfss://data@stsparsqlwesteurope.dfs.core.windows.net/expedia'
                   """
spark.sql(expedia_ddl_query)

// create hotel-weather delta table
val hotel_weather_ddl_query = """CREATE TABLE IF NOT EXISTS M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER 
                   USING DELTA
                   LOCATION 'abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather'
                   """
spark.sql(hotel_weather_ddl_query)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 1. Top 10 hotels with max absolute temperature difference by month.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.1 Save intermediate data and create delta table for query

// COMMAND ----------

// select and save intermediate data
val hotel_weather_temp_query = """SELECT
                                     id,
                                     name,
                                     address,
                                     YEAR(wthr_date) AS year,
                                     MONTH(wthr_date) AS month,
                                     MAX(avg_tmpr_c) AS max_temp,
                                     MIN(avg_tmpr_c) AS min_temp
                                 FROM M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER
                                 GROUP BY id, name, address, YEAR(wthr_date), MONTH(wthr_date)
                                 """

spark.sql(hotel_weather_temp_query)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather-temp")

// create intermediate delta table
val hotel_weather_temp_ddl_query =
  """CREATE TABLE IF NOT EXISTS M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_TEMP
                   USING DELTA
                   LOCATION 'abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather-temp'
                   """
spark.sql(hotel_weather_temp_ddl_query)


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Execution plan for hotel_weather_temp_query
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC *(2) HashAggregate(keys=[id#423, name#426, address#417, year(cast(wthr_date#427 as date))#440, month(cast(wthr_date#427 as date))#441], functions=[max(avg_tmpr_c#418), min(avg_tmpr_c#418)])
// MAGIC +- Exchange hashpartitioning(id#423, name#426, address#417, year(cast(wthr_date#427 as date))#440, month(cast(wthr_date#427 as date))#441, 200), ENSURE_REQUIREMENTS, [id=#109]
// MAGIC    +- *(1) HashAggregate(keys=[id#423, name#426, address#417, year(cast(wthr_date#427 as date)) AS year(cast(wthr_date#427 as date))#440, month(cast(wthr_date#427 as date)) AS month(cast(wthr_date#427 as date))#441], functions=[partial_max(avg_tmpr_c#418), partial_min(avg_tmpr_c#418)])
// MAGIC       +- *(1) ColumnarToRow
// MAGIC          +- FileScan parquet m07_sparksql_azure_db.hotel_weather[address#417,avg_tmpr_c#418,id#423,name#426,wthr_date#427] Batched: true, DataFilters: [], Format: Parquet, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<address:string,avg_tmpr_c:double,id:string,name:string,wthr_date:string>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 1.2 Final query

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT id,
// MAGIC       name,
// MAGIC       address,
// MAGIC       year,
// MAGIC       month,
// MAGIC       ABS(max_temp - min_temp) as max_diff,
// MAGIC       max_temp,
// MAGIC       min_temp
// MAGIC FROM M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_TEMP
// MAGIC ORDER BY max_diff desc
// MAGIC LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 2. Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2.1 Save intermediate data and create delta table for query

// COMMAND ----------

import org.apache.commons.lang3.StringUtils.isBlank
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}

// custom function for calculating unique YYYY-MM strings from check-in and check-out dates
def getYearMonths(start: String, end: String): Seq[String] = {
  if (isBlank(start) || isBlank(end)) return Seq.empty

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val startDate = LocalDate.parse(start, formatter)
  val endDate = LocalDate.parse(end, formatter)

  val startMonth = YearMonth.from(startDate)
  val endMonth = YearMonth.from(endDate)

  val months = collection.mutable.ListBuffer[String]()

  var currentMonth = startMonth
  while (!currentMonth.isAfter(endMonth)) {
    months += currentMonth.format(DateTimeFormatter.ofPattern("yyyy-MM"))
    currentMonth = currentMonth.plusMonths(1)
  }

  months.distinct
}

// register custom function so it could be used in SparkSQL query
spark.udf.register("getYearMonths", udf(getYearMonths _))

// COMMAND ----------

// select and save intermediate data
val hotel_months_count =
  """SELECT *
     FROM (
       SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY month_visited
               ORDER BY visit_count DESC
             ) as rank
       FROM (
         SELECT *,
                COUNT(*) as visit_count
         FROM (
           SELECT
               hotel_id,
               explode(getYearMonths(srch_ci, srch_co)) AS month_visited
           FROM M07_SPARKSQL_AZURE_DB.EXPEDIA
         )
         GROUP BY hotel_id, month_visited
       )
     )
  """

spark.sql(hotel_months_count)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-months-count")

// create intermediate delta table
val hotel_months_count_ddl_query =
  """CREATE TABLE IF NOT EXISTS M07_SPARKSQL_AZURE_DB.HOTEL_MONTHS_COUNT
                   USING DELTA
                   LOCATION 'abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-months-count'
                   """

spark.sql(hotel_months_count_ddl_query)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Execution plan for hotel_months_count
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC Window [row_number() windowspecdefinition(month_visited#441, visit_count#4L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#5], [month_visited#441], [visit_count#4L DESC NULLS LAST]
// MAGIC +- *(4) Sort [month_visited#441 ASC NULLS FIRST, visit_count#4L DESC NULLS LAST], false, 0
// MAGIC    +- Exchange hashpartitioning(month_visited#441, 200), ENSURE_REQUIREMENTS, [id=#136]
// MAGIC       +- *(3) HashAggregate(keys=[hotel_id#440L, month_visited#441], functions=[count(1)])
// MAGIC          +- Exchange hashpartitioning(hotel_id#440L, month_visited#441, 200), ENSURE_REQUIREMENTS, [id=#132]
// MAGIC             +- *(2) HashAggregate(keys=[hotel_id#440L, month_visited#441], functions=[partial_count(1)])
// MAGIC                +- Generate explode(UDF(srch_ci#433, srch_co#434)), [hotel_id#440L], false, [month_visited#441]
// MAGIC                   +- *(1) Filter ((size(UDF(srch_ci#433, srch_co#434), true) > 0) AND isnotnull(UDF(srch_ci#433, srch_co#434)))
// MAGIC                      +- *(1) ColumnarToRow
// MAGIC                         +- FileScan parquet m07_sparksql_azure_db.expedia[srch_ci#433,srch_co#434,hotel_id#440L] Batched: true, DataFilters: [(size(UDF(srch_ci#433, srch_co#434), true) > 0), isnotnull(UDF(srch_ci#433, srch_co#434))], Format: Parquet, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 2.2 Final query

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM M07_SPARKSQL_AZURE_DB.HOTEL_MONTHS_COUNT
// MAGIC WHERE rank <= 10
// MAGIC ORDER BY month_visited, rank

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 3. For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3.1 Save intermediate data and create delta table for query

// COMMAND ----------

// select and save intermediate data
val hotel_weather_avg_and_trend_query =
  """ SELECT
    *
  FROM
    (
      SELECT
        expedia.hotel_id,
        expedia.srch_ci,
        expedia.srch_co,
        CAST(
          AVG(hotel_weather.avg_tmpr_c) AS DECIMAL(3, 2)
        ) AS avg_temperature,
        (
          CAST(
            (
              MAX(
                CASE WHEN to_date(hotel_weather.wthr_date, 'yyyy-MM-dd') = to_date(expedia.srch_ci, 'yyyy-MM-dd') THEN hotel_weather.avg_tmpr_c END
              ) - MAX(
                CASE WHEN to_date(hotel_weather.wthr_date, 'yyyy-MM-dd') = to_date(expedia.srch_co, 'yyyy-MM-dd') THEN hotel_weather.avg_tmpr_c END
              )
            ) AS DECIMAL(3, 2)
          )
        ) AS weather_trend,
        MAX(
          CASE WHEN  to_date(
            hotel_weather.wthr_date, 'yyyy-MM-dd'
          ) = to_date(expedia.srch_ci, 'yyyy-MM-dd') THEN hotel_weather.avg_tmpr_c END
        ) AS check_in_temperature,
        MAX(
          CASE WHEN to_date(
            hotel_weather.wthr_date, 'yyyy-MM-dd'
          ) = to_date(expedia.srch_co, 'yyyy-MM-dd') THEN hotel_weather.avg_tmpr_c END
        ) AS check_out_temperature
      FROM
        expedia
        JOIN hotel_weather ON expedia.hotel_id = hotel_weather.id
        AND to_date(
          hotel_weather.wthr_date, 'yyyy-MM-dd'
        ) BETWEEN to_date(expedia.srch_ci, 'yyyy-MM-dd')
        AND to_date(expedia.srch_co, 'yyyy-MM-dd')
      WHERE
        DATEDIFF(
          expedia.srch_co, expedia.srch_ci
        ) > 7
      GROUP BY
        expedia.hotel_id,
        expedia.srch_ci,
        expedia.srch_co
    )
  WHERE
    check_in_temperature IS NOT NULL
    AND check_out_temperature IS NOT NULL
    AND avg_temperature IS NOT NULL
     """

spark.sql(hotel_weather_avg_and_trend_query)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather-and-trend")

// create intermediate delta table
val hotel_months_count_ddl_query =
  """CREATE TABLE IF NOT EXISTS M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_AND_TREND
                   USING DELTA
                   LOCATION 'abfss://data@stsparsqlwesteurope.dfs.core.windows.net/hotel-weather-and-trend'
                   """
spark.sql(hotel_months_count_ddl_query)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Execution plan for hotel_weather_avg_and_trend_query
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC *(3) Filter ((isnotnull(check_in_temperature#2) AND isnotnull(check_out_temperature#3)) AND isnotnull(avg_temperature#0))
// MAGIC +- *(3) HashAggregate(keys=[hotel_id#436L, srch_ci#429, srch_co#430], functions=[avg(avg_tmpr_c#828), max(CASE WHEN (cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) = cast(gettimestamp(srch_ci#429, yyyy-MM-dd, Some(Europe/Belgrade), false) as date)) THEN avg_tmpr_c#828 END), max(CASE WHEN (cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) = cast(gettimestamp(srch_co#430, yyyy-MM-dd, Some(Europe/Belgrade), false) as date)) THEN avg_tmpr_c#828 END)])
// MAGIC    +- Exchange hashpartitioning(hotel_id#436L, srch_ci#429, srch_co#430, 200), ENSURE_REQUIREMENTS, [id=#237]
// MAGIC       +- *(2) HashAggregate(keys=[hotel_id#436L, srch_ci#429, srch_co#430], functions=[partial_avg(avg_tmpr_c#828), partial_max(CASE WHEN (cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) = cast(gettimestamp(srch_ci#429, yyyy-MM-dd, Some(Europe/Belgrade), false) as date)) THEN avg_tmpr_c#828 END), partial_max(CASE WHEN (cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) = cast(gettimestamp(srch_co#430, yyyy-MM-dd, Some(Europe/Belgrade), false) as date)) THEN avg_tmpr_c#828 END)])
// MAGIC          +- *(2) Project [srch_ci#429, srch_co#430, hotel_id#436L, avg_tmpr_c#828, wthr_date#837]
// MAGIC             +- *(2) BroadcastHashJoin [hotel_id#436L], [cast(id#833 as bigint)], Inner, BuildRight, ((cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) >= cast(gettimestamp(srch_ci#429, yyyy-MM-dd, Some(Europe/Belgrade), false) as date)) AND (cast(gettimestamp(wthr_date#837, yyyy-MM-dd, Some(Europe/Belgrade), false) as date) <= cast(gettimestamp(srch_co#430, yyyy-MM-dd, Some(Europe/Belgrade), false) as date))), false
// MAGIC                :- *(2) Filter (((isnotnull(srch_co#430) AND isnotnull(srch_ci#429)) AND (datediff(cast(srch_co#430 as date), cast(srch_ci#429 as date)) > 7)) AND isnotnull(hotel_id#436L))
// MAGIC                :  +- *(2) ColumnarToRow
// MAGIC                :     +- FileScan parquet m07_sparksql_azure_db.expedia[srch_ci#429,srch_co#430,hotel_id#436L] Batched: true, DataFilters: [isnotnull(srch_co#430), isnotnull(srch_ci#429), (datediff(cast(srch_co#430 as date), cast(srch_c..., Format: Parquet, PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>
// MAGIC                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, string, false] as bigint)),false), [id=#231]
// MAGIC                   +- *(1) Filter isnotnull(id#833)
// MAGIC                      +- *(1) ColumnarToRow
// MAGIC                         +- FileScan parquet m07_sparksql_azure_db.hotel_weather[avg_tmpr_c#828,id#833,wthr_date#837] Batched: true, DataFilters: [isnotnull(id#833)], Format: Parquet, PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<avg_tmpr_c:double,id:string,wthr_date:string>
// MAGIC                         

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 3.2 Final query

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_AND_TREND
// MAGIC LIMIT 20
