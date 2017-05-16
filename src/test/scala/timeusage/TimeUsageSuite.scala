package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("Columns are classified correctly") {

    val columns = List("t010101", "t110011001", "t05005050", "t180500505", "t02002002", "t1801", "t1899")

    val (primNeedCols, workCols, otherCols) = TimeUsage.classifiedColumns(columns)

    assert(primNeedCols == List(new Column("t010101"), new Column("t110011001"), new Column("t1801")), "Primary needs columns not classified correctly")
    assert(workCols == List(new Column("t05005050"), new Column("t180500505")))
    assert(otherCols == List(new Column("t02002002"), new Column("t1899")))
  }

  test("Total hours are classified and summed correctly") {

    val spark = SparkSession.builder().appName("Test-TimeUsage").getOrCreate()

    val cols = List("tucaseid", "telfs", "tesex", "teage", "t010101", "t110011001", "t1801", "t05005050", "t180500505",
      "t02002002", "t1899")

    val fields = StructField(cols.head, StringType, nullable = false) :: cols.tail.map(StructField(_, DoubleType, nullable = false))

    val rdd = spark.sparkContext.parallelize(List(Row("x", 2d, 1d, 25d, 10d, 14d, 17d, 93d, 13d, 14d, 15d)))

    val df = spark.createDataFrame(rdd, StructType(fields))

    val (primNeedCols, workCols, otherCols) = TimeUsage.classifiedColumns(cols)

    val summary = TimeUsage.timeUsageSummary(primNeedCols, workCols, otherCols, df)
    val summaryList = summary.collect().toList

    assert(summaryList(0)(3) == 41d / 60d, "Primary needs hours incorrectly summed")
    assert(summaryList(0)(4) == 106d / 60d, "Work hours incorrectly summed")
    assert(summaryList(0)(5) == 29d / 60d, "Other hours incorrectly summed")
  }

  test("TimeUsageGrouped groups time correctly") {

    val spark = SparkSession.builder().appName("Test-TimeUsageGrouped").getOrCreate()

    val cols = List("tucaseid", "telfs", "tesex", "teage", "t010101", "t110011001", "t1801", "t05005050", "t180500505",
      "t02002002", "t1899")

    val fields = StructField(cols.head, StringType, nullable = false) :: cols.tail.map(StructField(_, DoubleType, nullable = false))

    val rdd = spark.sparkContext.parallelize(List(
      Row("x", 2d, 1d, 25d, 10d, 14d, 17d, 93d, 13d, 14d, 15d),
      Row("y", 2d, 1d, 25d, 10d, 14d, 17d, 93d, 13d, 14d, 15d)))

    val df = spark.createDataFrame(rdd, StructType(fields))

    val (primNeedCols, workCols, otherCols) = TimeUsage.classifiedColumns(cols)

    val summary = TimeUsage.timeUsageSummary(primNeedCols, workCols, otherCols, df)

    val summaryGrouped = TimeUsage.timeUsageGrouped(summary)

    val summaryGroupedList = summaryGrouped.collect().toList

    assert(summaryGroupedList(0)(3) == 0.7, "PrimaryNeeds grouped for age, sex and working status not grouped correctly")
    assert(summaryGroupedList(0)(4) == 1.8, "Work grouped for age, sex and working status not grouped correctly")
    assert(summaryGroupedList(0)(5) == 0.5, "Other grouped for age, sex and working status not grouped correctly")
  }
}
