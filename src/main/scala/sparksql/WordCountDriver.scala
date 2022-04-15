package sparksql

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
    val sparkSession = SparkSession.builder().master(args(0)).appName("WordCountDriverSQL").getOrCreate() // RETURN TYPE = SparkSession

    // wordcountRDD [Resilient Distributed Dataset] Created by Spark Driver Job
    val wordCountRDD = sparkSession.sparkContext // Return TYPE = SparkContext

      // INPUT :-
      // this is a book
      // a book
      .textFile(args(1)) // Return TYPE = RDD[String]

      // OUTPUT = (this , is , a , book, a, book)
      .flatMap(line => line.split(" ")) // Return TYPE = RDD[String]

      // OUTPUT = (WordCount(this), WordCount(is), WordCount(a), WordCount(book), WordCount(a), WordCount(book))
      .map(word => WordCount(word)) // Return TYPE = RDD[WordCount]

    // infer the schema from RDD and Convert it into Spark Dataframe of Row Type
    import sparkSession.implicits._
    val wordCountDF = wordCountRDD.toDF() // Return TYPE = DataFrame[Row]

    // print the schema
    wordCountDF.printSchema()

    // register the dataset as a table/view
    wordCountDF.createOrReplaceTempView("words")

    // execute sql query
    val resultDF = sparkSession.sql("select word, count(word) as count from words group by word") // Return TYPE = DataFrame[Row]

    // display the result (top 20 rows by default)
    resultDF.show()

    // RESULT = collecting resultDF data as an Array[Row] and printing each line as an output
    resultDF.collect().foreach(println)

    // close the spark session
    sparkSession.close()
  }
}

// Regular classes which are immutable by default and decomposable through pattern matching.
// It does not use new keyword to instantiate object.
// All the parameters listed in the case class are public and immutable by default.
case class WordCount(word: String)
