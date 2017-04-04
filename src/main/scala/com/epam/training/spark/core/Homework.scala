package com.epam.training.spark.core

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] =
    sc.textFile(rawDataPath).filter(!_.startsWith("#")).map(_.split(";", 7).toList)

  def findErrors(rawData: RDD[List[String]]): List[Int] =
    rawData.collect.map(e => e count (_.equals(""))).toList

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] =
    rawData.map(e => Climate(e(0), e(1), e(2), e(3), e(4), e(5), e(6)))

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] =
    climateData.filter(e => e.observationDate.getMonthValue == month && e.observationDate.getDayOfMonth == dayOfMonth).map(e => e.meanTemperature.value)

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    val climateDataArray = climateData.collect
    var matchingClimateData: Array[Climate] = Array()

    for (i <- climateDataArray.indices) {
      if (climateDataArray(i).observationDate.getMonthValue == month && climateDataArray(i).observationDate.getDayOfMonth == dayOfMonth) {
        matchingClimateData = matchingClimateData ++ Array(climateDataArray(i - 1), climateDataArray(i), climateDataArray(i + 1))
      }
    }

    matchingClimateData.map(e => e.meanTemperature.value).sum / matchingClimateData.size
    climateDataArray.map(e => e.meanTemperature.value).sum / climateDataArray.size
  }


}


