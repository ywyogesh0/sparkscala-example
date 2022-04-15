package sparkcore

import org.apache.spark.SparkContext

/**
 * WordCountDriver object used to submit spark job on hadoop cluster where spark is installed
 *
 * -master: The master URL for the cluster OR local[n] cores
 * -text-file-path: Used by Spark driver job to load the data and distribute partitions across workers
 */
object StockPriceDriver {

  def main(args: Array[String]): Unit = {
    // check the args count
    if (args.length != 2) {
      println("Spark Driver job needs at-least 2 arguments <master> <text-file-path>")
    }

    // maxStockPriceRDD [Resilient Distributed Dataset] Created by Spark Driver Job
    val maxStockPriceRDD = new SparkContext(args(0), "MaxStockPriceDriverCore") // Return TYPE = SparkContext

      // INPUT :-
      // ABC 2.5
      // DEF 3.4
      // ABC 5.3
      .textFile(args(1)) // Return TYPE = RDD[String]

      // OUTPUT = ((ABC,2.5) , (DEF,3.4) , (ABC,5.3))
      .map(line => {
        val items = line.split(" ") // Return TYPE = String[]
        (items(0), items(1).toFloat)
      }) // Return TYPE = RDD[(String, Float)]

      // OUTPUT = ((ABC,5.3) , (DEF,3.4))
      .reduceByKey((x, y) => {
        if (y >= x) y else x
      }) // Return TYPE = RDD[(String, Float]

    // RESULT = collecting maxStockPriceRDD data as an Array[(String, Float)] and printing each line as an output
    maxStockPriceRDD.collect().foreach(println)
  }
}
