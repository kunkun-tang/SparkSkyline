package probSkyline.query

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;
import com.typesafe.config.ConfigFactory

/*
 * Using the basic filter to prune data firstly.
 */
class MediumQuery(var file: String){

	def fileName = file;

	/*
	 * read original data file's data to memory.
	 * from file to itemList, which represents the list of items.
	 */
	def getItemList = Util.getItemList(fileName);

	/*
	 * compProb is target to use naive way to compute probability
	 * of objects.
	 */
	def compProb(itemList: List[Item]){
		val list = new scala.collection.mutable.ListBuffer[(Int, Double)]();
		println("The size of items in this partition = "+ itemList.length);
		var satisfied = 0;
		var count = 0
		for(aItem <- itemList){
			var objSkyProb = 0.0
			// if(count%10 == 0) println("----- count: "+count + "-------");
			count += 1;
			for(aInst <- aItem.instances){
				var instSkyProb = 1.0;
				for(oItem <- itemList; if oItem.objID != aItem.objID){
					var itemAddition = 0.0;
					for(oInst <- oItem.instances; if oInst.checkDomination(aInst) == true){
						itemAddition += oInst.prob
					}
					instSkyProb *= (1- itemAddition)
				}
				aInst.instSkyProb = instSkyProb
				objSkyProb += aInst.prob * aInst.instSkyProb
			}
			aItem.objSkyProb = objSkyProb
			if(objSkyProb > 0.0){

				satisfied += 1;
			//	println("objSkyProb = "+ objSkyProb);
				// println(aItem.toString);
				list += ((aItem.objID, objSkyProb))
			}
		}

		val res = list.sortWith((x,y)=> x._2.compareTo(y._2)>0);
		println(res)
		println("=========================================")
		println("number of prob skyline equal to 1 = " + res.count(elem => elem._2 == 1.0));
		// println(satisfied + " items remained in the partition");
	}
}

class MediumQueryNoWRTree(var itemList: List[Item]){

	/*
	 * compProb is target to use naive way to compute probability
	 * of objects.
	 */
	import MediumQuery._;

	def compProb() = {
		val list = new scala.collection.mutable.ListBuffer[Int]();
		println("The size of items in this partition = "+ itemList.length);
		var satisfied = 0;
		var count = 0
		for(aItem <- itemList){
			var objSkyProb = 0.0
			// if(count%10 == 0) println("----- count: "+count + "-------");
			count += 1;
			for(aInst <- aItem.instances){
				var instSkyProb = 1.0;
				for(oItem <- itemList; if oItem.objID != aItem.objID){
					var itemAddition = 0.0;
					for(oInst <- oItem.instances; if oInst.checkDomination(aInst) == true){
						itemAddition += oInst.prob
					}
					instSkyProb *= (1- itemAddition)
				}
				aInst.instSkyProb = instSkyProb
				objSkyProb += aInst.prob * aInst.instSkyProb
			}
			aItem.objSkyProb = objSkyProb
			if(objSkyProb > lowerBoundProb){

				satisfied += 1;
			//	println("objSkyProb = "+ objSkyProb);
				// println(aItem.toString);
				list += aItem.objID
			}
		}

		// println(satisfied + " items remained in the partition");
		list
	}
}

object MediumQuery{

	val conf = ConfigFactory.load;
	def dim = conf.getInt("Query.dim");
	def lowerBoundProb = conf.getDouble("Query.lowerBoundProb");

  var startTime = 0L
  def start(){
      startTime = System.nanoTime
  }

  def end(str: String){
      println(str+ " time: "+(System.nanoTime-startTime)/1e6+"ms")
  }
}