/* SparkApp.scala */
package spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import com.typesafe.config.ConfigFactory

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.genData.SplitData;
import probSkyline.query.OptimizedQuerySpark;

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

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
    val srcData = sc.textFile(srcFile, numMapper)

    // transform every line to instance.
    val instData = srcData.map(line => {
    	// println(line)
    	Util.stringToInstance(line)}).cache()

    /*
     * Firstly, it computes the MAX_MIN Info from the original instance list.
     */
		val itemList = Util.getItemListFromRDD(instData)
		val outputLists = new ListBuffer[PartitionInfo]();
		for(i<-0 until numMapper)
			outputLists.append(new PartitionInfo(i))
		for(aItem <- itemList) SplitData.findExtreme(aItem, outputLists)

    // Group instance by predefined paritioning scheme.
    val individual = instData.groupBy(inst => Util.getPartition(inst))

    //--------------------test lenth of every partition------------------
    // individual.foreach(part => {
    // 	println(part._2.toList.length)
    // })

    individual.foreach(part => {
    	val itemMap = Util.getItemMapFromIterable(part._2);

    	val firstPhaseQuery = new OptimizedQuerySpark(part._1, itemMap, outputLists);
    	
    	firstPhaseQuery.compProb();
    })

    // println(individual.count());
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}