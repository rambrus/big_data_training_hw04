package com.epam.training.spark.core.domain

object MaybeDoubleType {

  object MaybeDouble {
    def apply(s: String): MaybeDouble = {
      s match {
        case "" => InvalidDouble
        case _ => ValidDouble(s.toDouble)
      }
    }
  }

  sealed abstract class MaybeDouble {
    val value: Double
  }

  case class ValidDouble(value: Double) extends MaybeDouble {
    override def toString: String = value.toString
  }

  case object InvalidDouble extends MaybeDouble {
    override def toString: String = "N/A"

    override val value: Double = Double.NaN
  }

}
