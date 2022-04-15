package sparkcore

import org.apache.spark.SparkContext

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

    // wordcountRDD [Resilient Distributed Dataset] Created by Spark Driver Job
    val wordCountRDD = new SparkContext(args(0), "WordCountDriverCore") // Return TYPE = SparkContext

      // INPUT :-
      // this is a book
      // a book
      .textFile(args(1)) // Return TYPE = RDD[String]

      // OUTPUT = (this , is , a , book, a, book)
      .flatMap(line => line.split(" ")) // Return TYPE = RDD[String]

      // OUTPUT = ((this,1) , (is,1) , (a,1) , (book,1), (a,1), (book,1))
      .map(word => (word, 1)) // Return TYPE = RDD[(String, Int]

      // OUTPUT = ((this,1) , (is,1) , (a,2) , (book,2))
      .reduceByKey((x, y) => x + y) // Return TYPE = RDD[(String, Int]

    // RESULT = collecting wordCountRDD data as an Array[(String, Int)] and printing each line as an output
    wordCountRDD.collect().foreach(println)
  }
}
