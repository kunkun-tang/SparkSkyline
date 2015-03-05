package probSkyline.query

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import scala.util.Random
import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;
import scala.collection.mutable.HashMap;
import scala.math;
import com.typesafe.config.ConfigFactory
import xxl.core.spatial.rectangles.DoublePointRectangle;
import scala.collection.JavaConversions.mapAsScalaMap;
import xxl.core.spatial.points.DoublePoint;

class WRTree(val instList: List[Instance], val testArea: String){

	import WRTree._

 	val rand = new Random(System.currentTimeMillis());

 	// Origin is the (0,0) origin in the x-y axis.
 	var origin: DoublePoint = null;
	var sortedRectInfo: List[(DoublePoint, RectInfo)] = null;
	val rectangleMap = new HashMap[DoublePoint, RectInfo]();

	/*
	 * Init Function iterates every instance and store instance information
	 * to rectangles. Then these info can be repeatedly applied.
	 */
	def init(){
		origin = new DoublePoint(Array.ofDim[Double](dim));

		val pivotList = getPivotPoints(5);

		pivotList.foreach{ pivotPoint => 
			rectangleMap += pivotPoint -> new RectInfo()
		};

		instList.foreach{ inst => {

			val pt = new DoublePoint(inst.pt.coordinates);
			rectangleMap.foreach{ case (pivotPoint, rectInfo)=>
				if(checkDomination(pt, pivotPoint)) rectInfo.update(inst)
			}
		}}
		sortedRectInfo = rectangleMap.toSeq.sortWith( (x,y)=> y._2.size.compareTo(y._2.size)>0 ).toList;	
	}

	/**
	 * Given the required number of pivot points, getPivotPoints returns
	 * the a combination of pivot point list.
	 */
	def getPivotPoints(numPivotPoint: Int)={

		/*
		 * onMap: every combination -> optimization power
		 * pointList: make instList to pointList
		 */
		val opMap = HashMap[List[DoublePoint], Int]()
		val pointList = instList.map(a => new DoublePoint(a.pt.coordinates));
		for(randomNum <- 0 to 500){

			/*
			 * pivotList: one combination
			 * for loop finds elements from original dataset, and randomly selects
			 * combination from elements.
			 */
			var pivotList = List[DoublePoint]();
			var OP=0;
			for(k<- 0 until numPivotPoint){
				val aRandom = pointList(rand.nextInt(pointList.size)) ;
				pivotList = aRandom :: pivotList ;
			}

			/*
			 * rectangles returns the all rectangles with pivot points.
			 * Then we collects how many instances in each rectangle.
			 */
			val rectangles = pivotList.map{ a => new DoublePointRectangle(origin, a)};
			val rectangleMap = new HashMap[DoublePointRectangle, Int]();
			rectangles.foreach{ rect => rectangleMap += rect -> 0};

			pointList.foreach{ pt => {
				rectangles.foreach{ rect=>
					if(rect.contains(pt)) rectangleMap.update(rect, rectangleMap(rect)+1)
				}
			}}

			/*
			 * for each instance, it finds the best optimal pivot point, which has the
			 * most optimization power 
			 */
			for(pt <- pointList){
				OP += findOptimizationPower(rectangleMap, pt)
			}

			opMap += pivotList -> OP;
		}

		/*
		 * it finds the pivot point combination with the largest optimization power.
		 */
		val result = opMap.maxBy(_._2);
		println("The returned pivot point combination: " + result);
		result._1
	}

	/*
	 * findOptimizationPower looks for the best pivot point, which has the most optimization power.
	 * and it will return the optimization power.
	 */
  def findOptimizationPower(rectangleMap: HashMap[DoublePointRectangle, Int], pt: DoublePoint): Int = {

    val satMap = rectangleMap.
    	filter{case (elem, num) => 
    		checkDomination(elem.getCorner(true).asInstanceOf[DoublePoint], pt)==true
    	}
   	var retRect = null;
   	if(satMap.size>0) satMap.maxBy(_._2)._2
    else 0
  }


	def collectInfo() = {

		instList.foreach{ inst => {
			val pt = new DoublePoint(inst.pt.coordinates);
			val (leftRect, rightRect) = checkArea(pt);
			var aggregateMap = new HashMap[Int, Double]();
			var testInstList: List[Instance] = instList;

			if(leftRect != null)
				aggregateMap = rectangleMap(leftRect).aggregateMap.clone();

			if(rightRect != null)
				testInstList = rectangleMap(rightRect).instRectList.toList;

			for(testInst <- testInstList){
				val testpt = new DoublePoint(testInst.pt.coordinates);
				if(leftRect != null ){
					if(checkDomination(leftRect, testpt) && checkDomination(testpt, pt))
						update(testInst, aggregateMap);
				}
				else{
					if(checkDomination(testpt, pt))
						update(testInst, aggregateMap);
				}
			}

			// println(aggregateMap);
			inst.instSkyProb = multiplyMap(inst, aggregateMap);
			// println("inst prob = " + inst.instSkyProb)
		}}

		/*
		 * collect every item's prob value, 
		 * and check if the value is larger than the predefiend lower bound of prob
		 * if it does, yield candidate objdID to a list, and return to sparkApp.
		 */
		val map = Util.getItemMapFromIterable(instList);
		for{
			(k,v) <- map;
			if v.instances.foldLeft(0.0)((prob, inst)=> prob + inst.prob*inst.instSkyProb) > lowerBoundProb
		}yield k

	}

	def checkArea(aPoint: DoublePoint) = {

		var br = false;
		var leftRect: DoublePoint = null;
		var rightRect: DoublePoint = null;

		for( (pivotPoint, info) <- sortedRectInfo; if(br == false)){
			if( checkDomination(pivotPoint, aPoint) ){
				leftRect = pivotPoint;
				br = true;
			}
		}

		br = false;
		for( (pivotPoint, info) <- sortedRectInfo.reverse; if(br == false)){
			if( checkDomination(aPoint, pivotPoint) ){
				rightRect = pivotPoint;
				br = true;
			}
		}

		(leftRect, rightRect);
	}

	def update(inst: Instance, map: HashMap[Int, Double]){
		if( !map.contains(inst.objID)) 
			map += inst.objID -> 0.0;
		map(inst.objID) += inst.prob;
	}

	def multiplyMap(inst: Instance, map: HashMap[Int, Double])={
		var ret = 1.0;
		for((objID, prob)<- map; if inst.objID != objID)
			ret *= prob;
		ret;
	}

	def run() = {

  	start();
		init();
		end("part " + testArea + " initialization time: ")

  	start();
		val result = collectInfo();
		end("part " + testArea+ " query time: ")
		result
	}
}


object WRTree{

	val conf = ConfigFactory.load;
	def folderName = conf.getString("Query.partFolder");
	def dim = conf.getInt("Query.dim");
	def numPivot = conf.getInt("Query.numPivot");
	def lowerBoundProb = conf.getDouble("Query.lowerBoundProb");

	def checkDomination(pt1: Array[Double], pt2: Array[Double]): Boolean={
		var ret = true;
		for(i <- 0 until pt1.length)
			if(pt2(i) < pt1(i)) ret = false;
		ret
	}

	def checkDomination(pt1: DoublePoint, pt2: DoublePoint): Boolean= 
		checkDomination(pt1.getPoint().asInstanceOf[Array[Double]], pt2.getPoint().asInstanceOf[Array[Double]]);

  var startTime = 0L
  def start(){
      startTime = System.nanoTime
  }

  def end(str: String){
      println(str+ " time: "+(System.nanoTime-startTime)/1e6+"ms")
  }
}

class RectInfo{
	
	/* 
	 * aggregateMap is the sum of instance existing prob
	 * of cleanItemMap in OptimizedQuery.
	 */
	var size = 0;

	val instRectList = new ListBuffer[Instance]();

	val aggregateMap = new HashMap[Int, Double]();
	def update(inst: Instance){
		if( !aggregateMap.contains(inst.objID)) 
			aggregateMap += inst.objID -> 0.0;
		aggregateMap(inst.objID) += inst.prob;
		size += 1;
		instRectList += inst;
	}

	override def toString() = aggregateMap.toString
}