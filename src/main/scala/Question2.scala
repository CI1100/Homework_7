import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** Find the movies with the most ratings. */
object Question2
{
  /** Our main function where the action happens */

  def main(args: Array[String]) 
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
    
	// Create a SparkContext using every core of the local machine, named PopularMovies
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
							.master("local[2]")
							.appName("word_1")
							.getOrCreate()
	import spark.implicits._ 

	// Read in each rating line
    val df = spark.read.option("header", false).option("inferSchema", true).option("delimiter", "\t").csv("ml-100k/u.data")
    
	val movies_avg_ratings = df.groupBy("_c1").mean("_c2").orderBy($"avg(_c2)".desc)
	println("Average movies ratings")
    println(movies_avg_ratings.show())
  }
}