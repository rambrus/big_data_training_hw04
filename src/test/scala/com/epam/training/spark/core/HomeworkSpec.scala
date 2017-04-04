package com.epam.training.spark.core

import com.epam.hubd.spark.scala.core.util.RddComparator
import com.epam.training.spark.core.domain.Climate
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class HomeworkSpec extends FunSpec with BeforeAndAfterAll {

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/input.csv"

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("EPAM BigData training Spark Core homework test")
    .setIfMissing("spark.master", "local[2]")
    .setIfMissing("spark.sql.shuffle.partitions", "2")
  val sc = new SparkContext(sparkConf)

  val RAW_DATA = Array(
    List("1901-01-01", "-11.7", "-9.0", "-13.6", "2.2", "4", ""),
    List("1901-01-02", "-11.1", "-10.0", "-15.2", "3.0", "4", ""),
    List("1901-01-03", "-13.0", "-10.0", "-15.7", "", "", ""),
    List("1955-01-01", "-2.1", "-1.0", "-3.4", "2.5", "4", "0.6"),
    List("1955-01-02", "-2.3", "-1.5", "-3.1", "0.1", "4", "0.1"),
    List("1955-01-03", "-3.9", "-2.0", "-6.5", "5.3", "4", "0.0"),
    List("2010-01-01", "5.0", "7.8", "3.6", "", "", "0.7"),
    List("2010-01-02", "1.6", "5.8", "0.1", "0.1", "4", "1.4"),
    List("2010-01-03", "-1.9", "0.2", "-2.6", "0.0", "4", "0.5")
  )

  val CLIMATE_DATA = Array(
    Climate("1901-01-01", "-11.7", "-9.0", "-13.6", "2.2", "4", ""),
    Climate("1901-01-02", "-11.1", "-10.0", "-15.2", "3.0", "4", ""),
    Climate("1901-01-03", "-13.0", "-10.0", "-15.7", "", "", ""),
    Climate("1955-01-01", "-2.1", "-1.0", "-3.4", "2.5", "4", "0.6"),
    Climate("1955-01-02", "-2.3", "-1.5", "-3.1", "0.1", "4", "0.1"),
    Climate("1955-01-03", "-3.9", "-2.0", "-6.5", "5.3", "4", "0.0"),
    Climate("2010-01-01", "5.0", "7.8", "3.6", "", "", "0.7"),
    Climate("2010-01-02", "1.6", "5.8", "0.1", "0.1", "4", "1.4"),
    Climate("2010-01-03", "-1.9", "0.2", "-2.6", "0.0", "4", "0.5")
  )

  override def afterAll() {
    sc.stop()
  }

  describe("rdd") {
    describe("when reading from csv file") {
      it("should have a list of strings") {
        val actual = Homework.getRawDataWithoutHeader(sc, INPUT_BIDS_INTEGRATION)
        val expected = RAW_DATA
        RddComparator.printListDiff(expected, actual.collect())
        assert(actual.collect() === expected)
      }
    }

    describe("when counting missing data") {
      it("should summarize them") {
        val actual = Homework.findErrors(sc.parallelize(RAW_DATA))
        val expected = List(0, 0, 0, 0, 2, 2, 3)
        assert(actual === expected)
      }
    }

    describe("when mapped to Climate") {
      it("should contain Climate objects") {
        val actual = Homework.mapToClimate(sc.parallelize(RAW_DATA))
        val expected = CLIMATE_DATA
        RddComparator.printAnyDiff(expected, actual.collect())
        assert(actual.collect() === expected)
      }
    }

    describe("when checking temperature for a given day") {
      it("it should contain the temperatures for all observed dates") {
        val actual = Homework.averageTemperature(sc.parallelize(CLIMATE_DATA), 1, 2)
        val expected = Array(-11.1, -2.3, 1.6)
        RddComparator.printAnyDiff(expected, actual.collect())
        assert(actual.collect() === expected)
      }
    }

    describe("when predicting temperature for a given day") {
      it("it should return a temperature based on all observed dates including previous and next days") {
        val actual = Homework.predictTemperature(sc.parallelize(CLIMATE_DATA), 1, 2)
        val expected = -4.377777777777776
        assert(actual === expected)
      }
    }
  }


}
