package thoughtworks.citibike

import CitibikeTransformerUtils._
import org.apache.spark.sql.types.{DoubleType, StructField}
import thoughtworks.DefaultFeatureSpecWithSpark

class CitibikeTransformerUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Compute Distances") {
    scenario("test adding a distance column to our output dataframe") {
      Given("A dataframe with bike start and end locations")
      import spark.implicits._
      val citibikeBaseDataColumns = Seq(
        "tripduration", "starttime", "stoptime", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bikeid", "usertype", "birth_year", "gender"
      )
      val sampleCitibikeData = Seq(
        (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
        (1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
        (1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
      )
      val inputDF = sampleCitibikeData.toDF(citibikeBaseDataColumns: _*)

      When("I compute distances")
      val result = inputDF.computeDistances(spark)

      Then("I have a column representing the distance from start to end location")
      result.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
  }
  }
}

