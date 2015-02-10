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
import probSkyline.query._;

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap


import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.PropertyConfigurator
 
object SparkApp {


  class ExactPartitioner[V](numPart: Int) extends Partitioner {

	  def getPartition(key: Any): Int = {
	    val k = key.asInstanceOf[Int]
	    k
	  }

	  def numPartitions: Int = numPart
	}

  def main(args: Array[String]) {


    PropertyConfigurator.configure("/Users/ltang/ScalaProjects/SparkSkyline/src/main/resources/log4j.properties")
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info("Hello world")

  	val config = ConfigFactory.load;

    val srcFile = config.getString("Query.srcFile") // Should be some file on your system
    val numMapper = config.getInt("Query.splitNum")
    val dim = config.getInt("Query.dim");
    val lowerBoundProb = config.getDouble("Query.lowerBoundProb");


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
    individual.foreach(part => {
    	println("part" + part._1 + " num: " + part._2.toList.length)
    })

    /*
     * infInsts is an instance ID list including all instance ID still needed
     * in the next round of real skyline probability computation.
     * canObjs is candidate objects list which includes all candidates objects ID.
     */
    val (inflInsts, candObjs) = individual.map(part => {
    	val itemMap = Util.getItemMapFromIterable(part._2);

    	val firstPhaseQuery = new OptimizedQuerySpark(part._1, itemMap, outputLists);
      firstPhaseQuery.compProb();
    }).reduce((pair1, pair2) => (pair1._1 ++ pair2._1,  pair1._2 ++ pair2._2))

    /*
     * Transform list to Set for convenience of contains operator.
     */
    val (inflInstSet, canObjSet) = (inflInsts.toSet, candObjs.toSet)
    val remainInsts = instData.filter(inst => inflInstSet.contains(inst.instID) == true);
    println(inflInstSet.size + "  ------  "+canObjSet.size)

    /*
     * Candidate instance will be sorted by x value of n-dimensional instance.
     */
    val candInsts = instData.filter(inst => canObjSet.contains(inst.objID) == true)
                            .sortBy(inst=>inst.pt.coordinates(0))

    /*
     * After sorting, it uese the quantitle generator to group data into compatable
     ＊ partObjInsts's format: [(partID, List((Instance, Index)))]
     */
    val quantileGen = new Util.GenQuantilePartition(candInsts.count.toInt, numMapper)
    val partObjInsts = candInsts.zipWithIndex.groupBy{
     case (inst,index) => quantileGen.genPart(index.toInt)
    }
    // partObjInsts.foreach{case (id, list)=> println(" partObjInsts id "+ id + " list length=" + list.size)}

    /*
     * maxPointList generation: [partID, MAXPoint]
     * a. gather all candidate obj lists in a partition, looks for max coner point
     * b. collect (partID, maxPoint) as a list
     * Make the transformation RDD(elem)->Array(elem)
     */
    val maxPointList = partObjInsts.map{ case (partId, partInfo) =>
      
      val maxPoint = partInfo.map(_._1.pt).fold(new Point(dim))( (acc,point) =>{
        for(j<-0 until dim) acc(j) = math.max(acc(j), point(j))
        acc
      });
      (partId, maxPoint)
    }.collect()

    // println(maxPointList)
    /*
     * partInflInsts format: [(partID, List(partID, instance))]
     * groupBy partID of every instance
     ### for comprehension doesn't work in Spark, since withFilter is not suppered in Spark.
     */
    val partInflInsts = remainInsts.flatMap(elem =>{

      maxPointList.flatMap(tuple => 
        if(elem.pt.checkDomination(tuple._2) == true)Some((tuple._1, elem))
        else None
      )
    }).groupBy(_._1)

    // partInflInsts.foreach{case (id, list)=> println(" partInflInst id "+ id + " list length=" + list.size)}

    /*
     * The finalPartition's format will be [( (partID, List(partID, instance)),  (partID, List((Instance, Index))) )]
     * using zip to make instances with the same partition ID are grouped together.
     */
    val finalPartition = partInflInsts.zip(partObjInsts)
    // finalPartition.foreach{case (infPart, candPart)=> println(" infPart No.="+ infPart._1 + " candPart No.=" + candPart._1)}


    /*
     *----finalPartition => [Iterable[instance]] => [(objID, Iterable[Instance])] => [ (objID, objSkyProb)]
     */
    val allCandSkyProb = finalPartition.flatMap{case (infPart, candPart)=> {

      val secPhase = new SecondPhaseQuerySpark(infPart._2.map(_._2), candPart._2.map(_._1))
      secPhase.compProb();
    }}.groupBy(inst => inst.objID).map{

      case (objID, iterable)=> (objID, iterable.foldLeft(0.0)( (acc, inst) => acc+inst.prob*inst.instSkyProb))
    }

    /*
     * filter all required candidate objects whose skyProb is larger then bound
     */
    val finalObjSkyProb = allCandSkyProb.filter(_._2 > lowerBoundProb)

    println(" obj num = "+ finalObjSkyProb.count)
    // println(individual.count());
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}