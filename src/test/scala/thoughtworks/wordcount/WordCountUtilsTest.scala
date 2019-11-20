package thoughtworks.wordcount

import WordCountUtils._
import org.apache.spark.sql.{Dataset, Encoders}
import thoughtworks.DefaultFeatureSpecWithSpark

class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {
      import spark.implicits._
      Given("A dataset with simple values and whitespaces")
      val inputList = List(
        "one     two three"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I split on the whitespaces")
      val result = dataset.splitWords(spark).collectAsList()

      Then("I have a list of words")
      result should contain theSameElementsAs List("one", "two", "three")
    }

    scenario("test splitting a dataset of words by period") {
      import spark.implicits._
      Given("A dataset with simple values, whitespace, and periods")
      val inputList = List(
        "one.two. three"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I split the words")
      val result = dataset.splitWords(spark).collectAsList()

      Then("I have a list of words")
      result should contain theSameElementsAs List("one", "two", "three")
    }

    scenario("test splitting a dataset of words by comma") {
      import spark.implicits._
      Given("A dataset with simple values, whitespace, and periods")
      val inputList = List(
        "one,two,. three"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I split on the words")
      val result = dataset.splitWords(spark).collectAsList()

      Then("I have a list of words")
      result should contain theSameElementsAs List("one", "two", "three")
    }

    scenario("test splitting a dataset of words by hypen") {
      import spark.implicits._
      Given("A dataset with simple values, whitespace, hyphens, and periods")
      val inputList = List(
        "one-two,- three. "
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I split on the words")
      val result = dataset.splitWords(spark).collectAsList()

      Then("I have a list of words")
      result should contain theSameElementsAs List("one", "two", "three")
    }

    scenario("test splitting a dataset of words by semi-colon") {
      import spark.implicits._
      Given("A dataset with simple values, whitespace, hyphens, semi-colons, and periods")
      val inputList = List(
        "one;two;. three; "
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I split on the words")
      val result = dataset.splitWords(spark).collectAsList()

      Then("I have a list of words")
      result should contain theSameElementsAs List("one", "two", "three")
    }
  }

  feature("Count Words") {
    scenario("basic test case") {
      import spark.implicits._
      Given("A dataset with a single value")
      val inputList = List(
        "one"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I count the words")
      val result = dataset.countByWord(spark).collectAsList()

      Then("I have a count of each word")
      result should contain theSameElementsAs List(("one",1))
    }

    scenario("should not aggregate dissimilar words") {
      import spark.implicits._
      Given("A dataset with dissimilar values")
      val inputList = List(
        "one", "two", "two"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I count the words")
      val result = dataset.countByWord(spark).collectAsList()

      Then("I have a count of each word")
      result should contain theSameElementsAs List(("one",1), ("two",2))
    }

    scenario("test case insensitivity") {
      import spark.implicits._
      Given("A dataset with similar values of different cases")
      val inputList = List(
        "oNe", "one", "TwO", "two"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)

      When("I count the words")
      val result = dataset.countByWord(spark).collectAsList()

      Then("I have a count of each word")
      result should contain theSameElementsAs List(("one",2), ("two",2))
    }
  }

  feature("Sort Words") {
    scenario("test ordering words") {
      import spark.implicits._
      Given("A dataset with values not in alphabetical order")
      val inputList = List(
        "zebra","banana", "app", "apple", "banana"
      )
      val dataset: Dataset[String] = spark.createDataset(inputList)
      dataset.show()

      When("I count the words")
      val result = dataset.countByWord(spark).collectAsList()
      dataset.countByWord(spark).show()

      Then("I have a count of each word")
      println(result)
      result should contain theSameElementsInOrderAs List(("app", 1), ("apple",1), ("banana",2), ("zebra", 1))
    }
  }

}
