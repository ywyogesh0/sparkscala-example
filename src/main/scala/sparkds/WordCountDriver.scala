package sparkds

import org.apache.spark.sql.SparkSession

/**
 * WordCountDriver object used to submit spark job on hadoop cluster where spark is installed
 *
 * -master: The master URL for the cluster OR local[n] cores
 * -text-file-path: Used by Spark driver job to load the data and distribute partitions across workers
 */
object WordCountDriver {

  def main(args: Array[String]): Unit = {
    // check the args count
    if (args.length != 2) {
      println("Spark Driver job needs at-least 2 arguments <master> <text-file-path>")
    }

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().master(args(0)).appName("WordCountDriverDS").getOrCreate() // Return TYPE = SparkSession

    // wordRDD [Resilient Distributed Dataset] Created by Spark Driver Job
    val wordRDD = sparkSession.sparkContext // Return TYPE = SparkContext

      // INPUT :-
      // this is a book
      // a book
      .textFile(args(1)) // Return TYPE = RDD[String]

      // OUTPUT = (this , is , a , book, a, book)
      .flatMap(line => line.split(" ")) // Return TYPE = RDD[String]

      // OUTPUT = (Word(this), Word(is), Word(a), Word(book), Word(a), Word(book))
      .map(word => Word(word)) // Return TYPE = RDD[Word]

    // infer the schema from RDD and Convert it into Spark Dataset of String type
    import sparkSession.implicits._
    val wordDS = wordRDD.toDS() // Return TYPE = Dataset[Word]

    // print the schema
    wordDS.printSchema()

    // use spark sql functions to create Dataset[WordCount]
    import org.apache.spark.sql.functions._
    val resultDS = wordDS
      .groupBy("word")
      .agg(count("word").as("count"))
      .select("word", "count")
      .as[WordCount] // Return Type = Dataset[WordCount]

    // display the FULL result
    resultDS.show(resultDS.count().toInt, truncate = false)

    // RESULT = collecting resultDS data as an Array[WordCount] and printing each line as an output
    resultDS.collect().foreach(println)

    // close the spark session
    sparkSession.close()
  }
}

// Regular classes which are immutable by default and decomposable through pattern matching.
// It does not use new keyword to instantiate object.
// All the parameters listed in the case class are public and immutable by default.
case class Word(word: String)
case class WordCount(word: String, count: BigInt)
