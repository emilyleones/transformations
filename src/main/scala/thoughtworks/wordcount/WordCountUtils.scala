package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession): Dataset[String] = {
      import spark.implicits._
      dataSet
        .flatMap(_.split("[^\\w']"))
        .filter(!_.equals(""))
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      dataSet.as[String]
        .groupByKey(_.toLowerCase)
        .count
        .orderBy($"value")
    }
  }
}
