package sparksql

import org.apache.spark.sql.SparkSession

/**
 * StockPriceDriver object used to submit spark job on hadoop cluster where spark is installed
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

    // spark session - used for sql (encapsulates spark and sql context)
    val sparkSession = SparkSession.builder().master(args(0)).appName("MaxStockPriceDriverSQL").getOrCreate() // RETURN TYPE = SparkSession

    // maxStockPriceRDD [Resilient Distributed Dataset] Created by Spark Driver Job
    val maxStockPriceRDD = sparkSession.sparkContext // Return TYPE = SparkContext

      // INPUT :-
      // ABC 2.5
      // DEF 3.4
      // ABC 5.3
      .textFile(args(1)) // Return TYPE = RDD[String]

      // OUTPUT = (Stocks(ABC,2.5) , Stocks(DEF,3.4) , Stocks(ABC,5.3))
      .map(line => {
        val items = line.split(" ") // Return TYPE = String[]
        Stocks(items(0), items(1).toFloat)
      }) // Return TYPE = RDD[Stocks]

    // infer the schema from RDD and Convert it into Spark Dataframe of Row Type
    import sparkSession.implicits._
    val maxStockPriceDF = maxStockPriceRDD.toDF() // Return TYPE = DataFrame[Row]

    // print the schema
    maxStockPriceDF.printSchema()

    // register the dataset as a table/view
    maxStockPriceDF.createOrReplaceTempView("stocks")

    // execute sql query
    val resultDF = sparkSession.sql("select stock, max(price) as max_price from stocks group by stock") // Return TYPE = DataFrame[Row]

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
case class Stocks(stock: String, price: Float)
