import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.log4j.{Level, Logger}


/** Find the movies with the most ratings. */
object Popular_1 
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
    
	// Read in each rating line
    val lines = sc.textFile("ml-100k/u.data")
    
	// Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
	// Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
	// Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
	// Sort
    val sortedMovies = flipped.sortByKey()
    
	// Collect and print results
    val results = sortedMovies.collect()
    results.foreach(println)
  }
}