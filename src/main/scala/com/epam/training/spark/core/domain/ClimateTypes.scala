package com.epam.training.spark.core.domain

import com.epam.training.spark.core.domain.MaybeDoubleType.MaybeDouble

object ClimateTypes {
  type Temperature = MaybeDouble
  val Temperature = MaybeDouble
  type PrecipitationAmount = MaybeDouble
  val PrecipitationAmount = MaybeDouble
  type SunshineHours = MaybeDouble
  val SunshineHours = MaybeDouble
}
