package com.epam.hubd.spark.scala.core.util

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD

object RddComparator {
  def printRddListDiff(expected: RDD[List[String]], actual: RDD[List[String]]) = {
    val actualArray = actual.collect
    val expectedArray = expected.collect
    printListDiff(expectedArray, actualArray)
  }
  def printRddDiff(expected: RDD[String], actual: RDD[String]) = {
    val actualArray = actual.collect
    val expectedArray = expected.collect
    printDiff(expectedArray, actualArray)
  }

  def printListDiff(expectedArray: Array[List[String]], actualArray: Array[List[String]]) = {
    printDiff(expectedArray.map(_.mkString(";")), actualArray.map(_.mkString(";")))
  }

  def printAnyDiff[T](expectedArray: Array[T], actualArray: Array[T]) = {
    printDiff(expectedArray.map(_.toString), actualArray.map(_.toString))
  }

  def printDiff(expectedArray: Array[String], actualArray: Array[String]) = {
    val expectedDiff = expectedArray.filter(x => !actualArray.contains(x)).mkString("\n")
    val actualDiff = actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n")
    if (!expectedDiff.isEmpty || !actualDiff.isEmpty) {
      println("")
      println("EXPECTED elements NOT available in actual set")
      println(expectedArray.filter(x => !actualArray.contains(x)).mkString("\n"))
      println("---")
      println("ACTUAL elements NOT available in expected set")
      println(actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n"))
    } else {
      println("There were no differences between expected and actual RDDs")
    }
  }

}
