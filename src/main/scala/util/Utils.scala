package util

import org.apache.commons.lang3.StringUtils.isBlank

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}

object Utils {

  /**
   * Calculates unique year-month strings between dates.
   *
   * @param start check-in date
   * @param end   check-out date
   * @return collection of unique year-month strings
   */
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
}
