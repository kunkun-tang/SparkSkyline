/* SparkApp.scala */
package spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import com.typesafe.config.ConfigFactory

import probSkyline.Util;


object SparkApp {


  class ExactPartitioner[V](numPart: Int) extends Partitioner {

	  def getPartition(key: Any): Int = {
	    val k = key.asInstanceOf[Int]
	    k
	  }

	  def numPartitions: Int = numPart
	}

  def main(args: Array[String]) {

  	val config = ConfigFactory.load;

    val srcFile = config.getString("Query.srcFile") // Should be some file on your system
    val numMapper = config.getInt("Query.splitNum")
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(srcFile, numMapper).cache()

    // transform every line to instance.
    val instData = srcData.map(line => {
    	// println(line)
    	Util.stringToInstance(line)})

    // Group instance by predefined paritioning scheme.
    val individual = instData.groupBy(inst => Util.getPartition(inst))

    println(individual.count());
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}