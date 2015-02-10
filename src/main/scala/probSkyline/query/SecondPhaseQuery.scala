package probSkyline.query

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.dataStructure.PartitionInfo;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.List;
import scala.collection.mutable.HashMap;
import com.typesafe.config.ConfigFactory

object SecondPhaseQuery{

	val conf = ConfigFactory.load;
	val dim = conf.getInt("Query.dim");

}

class SecondPhaseQuerySpark(val inflInsts: Iterable[Instance], val candInsts: Iterable[Instance]){
	
	val inflItemMap = Util.getItemMapFromIterable(inflInsts);

	def compProb() = {
		var satisfied = 0;
		for(inst <- candInsts){
			var instSkyProb = 1.0;
			for((id, oItem) <- inflItemMap; if id != inst.objID){

				var itemAddition = 0.0;
				for(oInst <- oItem.instances; if oInst.checkDomination(inst) == true)
						itemAddition += oInst.prob
					instSkyProb *= (1- itemAddition)
				}
			inst.instSkyProb = instSkyProb
		}
		candInsts
	}

}
