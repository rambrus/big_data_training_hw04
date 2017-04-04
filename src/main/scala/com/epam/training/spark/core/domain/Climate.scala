package com.epam.training.spark.core.domain

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.epam.training.spark.core.domain.ClimateTypes.{PrecipitationAmount, SunshineHours, Temperature}
import com.epam.training.spark.core.domain.PrecipitationType.Precipitation

/**
  * code    description
  * 0       fog drizzle, drizzle
  * 1       rain
  * 2       freezing rain, freezing drizzle
  * 3       rain shower
  * 4       snow, mixed rain and snow
  * 5       snow shower, shower of snow pellets
  * 6       hail, ice pellets
  * 7       thunderstorm (may occur without precipitation)
  * 8       snow-storm
  * 9       thunderstorm with hail
  */
object PrecipitationType {

  object Precipitation {
    def apply(s: String): Precipitation = {
      s match {
        case "0" => FogDrizzle
        case "1" => Rain
        case "2" => FreezingRain
        case "3" => RainShower
        case "4" => SnowRainMixed
        case "5" => HailIce
        case "6" => Thunderstorm
        case "7" => SnowStorm
        case "8" => ThunderstormHail
        case _ => NoPrecipitation
      }
    }
  }

  sealed abstract class Precipitation

  case object FogDrizzle extends Precipitation

  case object Rain extends Precipitation

  case object FreezingRain extends Precipitation

  case object RainShower extends Precipitation

  case object SnowRainMixed extends Precipitation

  case object SnowShower extends Precipitation

  case object HailIce extends Precipitation

  case object Thunderstorm extends Precipitation

  case object SnowStorm extends Precipitation

  case object ThunderstormHail extends Precipitation

  case object NoPrecipitation extends Precipitation

  val precipitationType = Seq(FogDrizzle, Rain, FreezingRain, RainShower, SnowRainMixed, SnowShower, HailIce, Thunderstorm, SnowStorm, ThunderstormHail)

}

case class Climate(
                    observationDate: LocalDate,
                    meanTemperature: Temperature,
                    maxTemperature: Temperature,
                    minTemperature: Temperature,
                    precipitationAmount: PrecipitationAmount,
                    precipitationType: Precipitation,
                    sunshineHours: SunshineHours
                  ) {
  override def toString: String = s"$observationDate,$meanTemperature,$maxTemperature,$minTemperature,$precipitationAmount,$precipitationType,$sunshineHours"
}

object Climate {

  val INPUT_DATE_FORMAT = DateTimeFormatter.ISO_DATE

  def apply(
             observationDate: String,
             meanTemperature: String,
             maxTemperature: String,
             minTemperature: String,
             precipitationAmount: String,
             precipitationType: String,
             sunshineHours: String
           ): Climate = new Climate(
    LocalDate.parse(observationDate, INPUT_DATE_FORMAT),
    Temperature(meanTemperature),
    Temperature(maxTemperature),
    Temperature(minTemperature),
    PrecipitationAmount(precipitationAmount),
    Precipitation(precipitationType),
    SunshineHours(sunshineHours)
  )

}