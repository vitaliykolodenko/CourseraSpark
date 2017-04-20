package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}


import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {



  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    val timeUsage = TimeUsage
    assert(!timeUsage.isPrimary("abc"))
    assert(timeUsage.isPrimary("t010102"))
    assert(!timeUsage.isPrimary("t088888"))
    assert(timeUsage.isPrimary("t18010202"))
  }
}
