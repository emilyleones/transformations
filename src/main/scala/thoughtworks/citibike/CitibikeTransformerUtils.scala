package thoughtworks.citibike

import org.apache.parquet.example.data.simple.DoubleValue
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  def computeHaversineDistance: (Double, Double, Double, Double) => Double = (startLatitude: Double, startLongitude: Double, endLatitude: Double, endLongitude: Double) => {
    val radiusOfEarth = 3958.8 // miles :(

    val startLatitudeInRadians = Math.toRadians(startLatitude)
    val endLatitudeInRadians = Math.toRadians(endLatitude)
    val latitudeDeltaInRadians = endLatitudeInRadians - startLatitudeInRadians
    val longitudeDeltaInRadians = Math.toRadians(endLongitude - startLongitude)

    val a = Math.pow(Math.sin(latitudeDeltaInRadians / 2), 2) +
      (Math.cos(startLatitudeInRadians) * Math.cos(endLatitudeInRadians) *
        Math.pow(Math.sin(longitudeDeltaInRadians / 2), 2))
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    val distance = radiusOfEarth * c
    BigDecimal(distance).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession) : DataFrame = {
      import spark.implicits._
      val haversineDistance = udf(computeHaversineDistance)
      dataSet.withColumn("distance", haversineDistance($"start_station_latitude", $"start_station_longitude", $"end_station_latitude", $"end_station_longitude"))
    }
  }
}

