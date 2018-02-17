
package spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.{
  Map,
  SynchronizedMap,
  HashMap
}
import scala.collection.mutable.MutableList
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

object classifier {
  
  // here I get the imageData by gathering all the neighbors of the current pixel position and then 
  // adding the brightness values of each to the list
  def getImageData(coordinate: String, imageData: List[String], zDim: Int): MutableList[String] = {
    val coord = coordinate.split(",")
    var xneighbor: Int = 21
    var yneighbor: Int = 21
    var zneighbor: Int = 7
    var x = coord(0).toInt
    var y = coord(1).toInt
    var z = coord(2).toInt

    var neighborlist = MutableList[String]()
    if ((x >= xneighbor && x < 100 - xneighbor) &&
      (y >= yneighbor && y < 100 - yneighbor) &&
      (z >= zneighbor && z < 10 - zneighbor)) {
      for (vx <- x - xneighbor to x + xneighbor) {
        for (vy <- y - yneighbor to y + yneighbor) {
          for (vz <- z - zneighbor to z + zneighbor) {
            var value = imageData(vz * 512 * 512 + vy * 512 + vx).split(":")(1)
            neighborlist += value.toString()
          }
        }
      }
    }
    return neighborlist;
  }

  // get all the rotatedNeighbors
  def rotate(Coordinate:String,imageData: List[String]):MutableList[String] = {
    val coord = Coordinate.split(",")
    var xneighbor: Int = 21
    var yneighbor: Int = 21
    var zneighbor: Int = 7
    var x = coord(0).toInt
    var y = coord(1).toInt
    var z = coord(2).toInt
     var neighborlist = MutableList[String]()
    for (vx <- x - xneighbor to x + xneighbor) {
      for (vy <- y - yneighbor to y + yneighbor) {
        for (vz <- z - zneighbor to z + zneighbor) {
          var value = imageData(-vy + vx + vz)
          neighborlist += value.toString()
          var value2 = imageData(vy - vx + vz)
          neighborlist += value2.toString()
          var value3 = imageData(-vy - vx + vz)
          neighborlist += value.toString()
        }
      }
    }
     return neighborlist
  }
  
  // here i am removing all the pixels that are close to the boundary
  def removePixels(coordinate: String, zDim: Int): Boolean = {
    val coord = coordinate.split(",")
    var neighbor: Int = 21
    var x = coord(0).toInt
    var y = coord(1).toInt
    var z = coord(2).toInt
    if ((x >= neighbor && x < 512 - neighbor) &&
      (y >= neighbor && y < 512 - neighbor) &&
      (z >= neighbor && z < 7 - neighbor)) return true;
    else return false;
  }

  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    for (i <- 1 to 6) {
      if (i != 5) {
        var distPath = "HW5-data/input/" + i + "_dist.tiff"

        // get the z-dimension from the current file
        val zDim: Int = LoadMultiStack.getzDimension(distPath)

        // get RDD of string containing the distance information of each pixel
        val coordinateDistValRDD = sc.parallelize(LoadMultiStack.parseInput(distPath, zDim).asScala.toList)

        // get the RDD(Coordinate,Distance) as the pairRDD information
        val outputRDD = coordinateDistValRDD.map(value => value.split(":"))
          .filter(node => (node(1) != 2 && node(1) != 3))
          .map { node =>
            (node(0), (if (node(1).toInt < 2) "0"
            else "1"))
          }

        var imagePath = "HW5-data/input/" + i + "_image.tiff"

        // get List(String) containing the list of adjacent nodes
        val coordinateImageValRDD2 = LoadMultiStack.parseInput(imagePath, zDim).asScala.toList

        // here we get an RDD(Coordinate,List(adjacent Nodes)) as the output
        val outputRDD2 = sc.parallelize(coordinateImageValRDD2.map(value => value.split(":"))
          .filter(node => removePixels(node(0), zDim))
          .map { node => (node(0), getImageData(node(0), coordinateImageValRDD2, zDim)) })

        // we join the above two output RDD's to get the common info of each pixel.
        // we get RDD(Coordinate,())
        val joined = outputRDD.join(outputRDD2).mapValues(node => node).sortByKey()

        // get only the values of the joinedRDD and save it as a file
        val joinedValues = joined.values
        joinedValues.saveAsTextFile(args(2))
      }

    }

    sc.stop()
  }
}