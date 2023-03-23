import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import util.Utils.getYearMonths

object App {
  val session: SparkSession = SparkSession
    .builder()
    .appName("homework2")
    .master("local[*]")
    .getOrCreate()

  // fill your properties first
  val inputPath: String = ""
  val outputPath: String = ""
  val clientId = ""
  val secret = ""
  val endpoint = ""
  val outputStorageAccountKey = ""

  def main(args: Array[String]): Unit = {

    ingestData()
    top10HotelsWithTempDifference()
    top10BusyHotelsForEachMonth()
    extendedStayWeatherTrend()

    session.close()
  }

  /**
   * Initial data ingestion
   */
  def ingestData(): Unit = {
    // input data params
    session.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
    session.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    session.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", clientId)
    session.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", secret)
    session.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", endpoint)

    // access key for ingested data storage account
    session.conf.set("spark.hadoop.fs.azure.account.key.stsparsqlwesteurope.dfs.core.windows.net", outputStorageAccountKey)

    // ingest expedia data into Azure storage
    session.read
      .format("avro")
      .load(inputPath + "/expedia")
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath + "/expedia")

    // ingest hotel-weather data into Azure storage
    session.read
      .format("parquet")
      .load(inputPath + "/hotel-weather")
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath + "hotel-weather")

    // create database
    session.sql("CREATE DATABASE IF NOT EXISTS M07_SPARKSQL_AZURE_DB")
    session.sql("USE M07_SPARKSQL_AZURE_DB")

    // create expedia delta table
    val expedia_ddl_query =
      s"""CREATE OR REPLACE TABLE M07_SPARKSQL_AZURE_DB.EXPEDIA
                       USING DELTA
                       LOCATION '$inputPath/expedia'
                       """
    session.sql(expedia_ddl_query)

    // create hotel-weather delta table
    val hotel_weather_ddl_query =
      s"""CREATE OR REPLACE TABLE M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER
                       USING DELTA
                       LOCATION '$inputPath/hotel-weather'
                       """
    session.sql(hotel_weather_ddl_query)
  }

  /**
   * Calculates top 10 hotels with max absolute temperature difference by month.
   */
  def top10HotelsWithTempDifference(): Unit = {
    // intermediate table for first query
    val hotel_weather_temp_query =
      """SELECT
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
    //session.sql(hotel_weather_temp_query).explain()

    session.sql(hotel_weather_temp_query)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(inputPath + "/hotel-weather-temp")

    val hotel_weather_temp_ddl_query =
      s"""CREATE OR REPLACE TABLE M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_TEMP
                       USING DELTA
                       LOCATION '$inputPath/hotel-weather-temp'
                       """
    session.sql(hotel_weather_temp_ddl_query)


    // the first query
    session.sql(
      """SELECT id,
                            name,
                            address,
                            year,
                            month,
                            ABS(max_temp - min_temp) as max_diff,
                            max_temp,
                            min_temp
                     FROM M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_TEMP
                     ORDER BY max_diff desc
                     LIMIT 10
                     """)
      .show()
  }

  /**
   * Claculates top 10 busy (e.g., with the biggest visits count) hotels for each month.
   * If visit dates refer to several months, it should be counted for all affected months.
   */
  def top10BusyHotelsForEachMonth(): Unit = {
    session.udf.register("getYearMonths", udf(getYearMonths _))

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
    // session.sql(hotel_months_count).explain()

    session.sql(hotel_months_count)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(inputPath + "/hotel-months-count")

    val hotel_months_count_ddl_query =
      s"""CREATE OR REPLACE TABLE M07_SPARKSQL_AZURE_DB.HOTEL_MONTHS_COUNT
                       USING DELTA
                       LOCATION '$inputPath/hotel-months-count'
                       """
    session.sql(hotel_months_count_ddl_query)


    // the second query
    session.sql(
      """
          SELECT *
          FROM M07_SPARKSQL_AZURE_DB.HOTEL_MONTHS_COUNT
          WHERE rank <= 10
          ORDER BY month_visited, rank
        """)
      .show()
  }

  /**
   * For visits with extended stay (more than 7 days) calculates weather trend
   * (the day temperature difference between last and first day of stay) and average.
   */
  def extendedStayWeatherTrend(): Unit = {

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

    //session.sql(hotel_weather_avg_and_trend_query).explain()

    session.sql(hotel_weather_avg_and_trend_query)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(inputPath + "/hotel-weather-and-trend")

    val hotel_months_count_ddl_query =
      s"""CREATE OR REPLACE TABLE M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_AND_TREND
                       USING DELTA
                       LOCATION '${inputPath}hotel-weather-and-trend'
                       """
    session.sql(hotel_months_count_ddl_query)


    // the third query
    session.sql(
      """
          SELECT *
          FROM M07_SPARKSQL_AZURE_DB.HOTEL_WEATHER_AND_TREND
          LIMIT 20
        """)
      .show()
  }
}
