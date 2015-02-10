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

object OptimizedQuery{

	val conf = ConfigFactory.load;
	val folderName = conf.getString("Query.partFolder");
	val dim = conf.getInt("Query.dim");

	def fileName(area: String) = "./" + folderName + "/" + area;

	var outputLists: ListBuffer[PartitionInfo] = null;
	def getItemList(area: String) = Util.getItemList(fileName(area));
	def getItemMap(area: String) = Util.getItemMap(fileName(area));
}


class OptimizedQuery(var area: String, val itemMap: HashMap[Integer, Item]){
	
	var outputLists: ListBuffer[PartitionInfo] = null;
	var cleanItemMap = new HashMap[Integer, Item]();

	def changeTestArea(cArea: String){
		area = cArea;
	}

	/*
	 * compProb is target to use optimized way to compute probability
	 * of objects.
	 */
	def compProb(){

	}

	/*
	 * Read MAXMIN file from external file link.
	 */
	def readMAXMIN(){
		import java.io.FileInputStream;
		import java.io.IOException;
		import java.io.File;
		import java.io.ObjectInputStream;

		class ObjectInputStreamWithCustomClassLoader(
		  fileInputStream: FileInputStream
		) extends ObjectInputStream(fileInputStream) {
		  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
		    try { Class.forName(desc.getName, false, getClass.getClassLoader) }
		    catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
		  }
		}


		try{
			val input = new FileInputStream(new File("./" + OptimizedQuery.folderName + "/MAX_MIN"));
			val in = new ObjectInputStreamWithCustomClassLoader(input);
			this.outputLists = in.readObject().asInstanceOf[ ListBuffer[PartitionInfo] ];
			in.close();
			input.close(); 
		}
		catch{
			case e: Exception => e.printStackTrace();
		}
	}

	def rule1(){
		if(outputLists != null){
			val areaInfo = outputLists(area.toInt);
			for( (idMax, ptMax) <- areaInfo.max ){
				if(itemMap(idMax).potentialSkyline == true){
					for{
						(idMin, ptMin) <- areaInfo.min
						if idMin != idMax 
						if itemMap(idMin).potentialSkyline == true
						if ptMax.checkDomination(ptMin) == true
					}itemMap(idMin).potentialSkyline = false;
				}
			}
	  }
	  removeAndGenerateNewList();
	}

	def removeAndGenerateNewList(){
//		println("before prune 1, the size of objects is " + itemMap.size);
		for( (objID, item) <- itemMap if item.potentialSkyline == true)
			cleanItemMap += objID -> item;
		println("after removing redundant items, the size decreases to " + cleanItemMap.size);
	}

	def rule2(){
		for( (objID, item)<- cleanItemMap; instance<- item.instances){
			for( (idMax, ptMax) <- outputLists(area.toInt).max  
				   if ptMax.checkDomination(instance.pt) == true
				 )instance.instPotentialSkyline = true;
		}
	}

	def rule3(){
		// val wrTree = new WRTree(cleanItemMap, area);
		// wrTree.run();
	}
}

class OptimizedQuerySpark(var area: Int, val itemMap: HashMap[Integer, Item], outputLists: ListBuffer[PartitionInfo]){
	
	var cleanItemMap = new HashMap[Integer, Item]();

	/*
	 * compProb is target to use optimized way to compute probability
	 * of objects.
	 */
	def compProb() = {
		rule1();
		val newInstList = rule2();

		/*
		 * canObjs is a list including all objs which meets the requirement.
		 * cleanItemMap includes all affecting instances.
		 */
		val candObjs = rule3(newInstList);
		(newInstList.map(x=>x.instID), candObjs)
	}

	def rule1(){
		if(outputLists != null){
			val areaInfo = outputLists(area);
			for( (idMax, ptMax) <- areaInfo.max ){
				if(itemMap(idMax).potentialSkyline == true){
					for{
						(idMin, ptMin) <- areaInfo.min
						if idMin != idMax 
						if itemMap(idMin).potentialSkyline == true
						if ptMax.checkDomination(ptMin) == true
					}itemMap(idMin).potentialSkyline = false;
				}
			}
	  }
	  removeAndGenerateNewList();
	}

	def removeAndGenerateNewList() = {
//		println("before prune 1, the size of objects is " + itemMap.size);
		for( (objID, item) <- itemMap if item.potentialSkyline == true)
			cleanItemMap += objID -> item;
		println("after removing redundant items, the size decreases to " + cleanItemMap.size);
	}

	def rule2()={
		for( (objID, item)<- cleanItemMap; instance<- item.instances){
			for( (idMax, ptMax) <- outputLists(area).max  
				   if ptMax.checkDomination(instance.pt) == true
				 )instance.instPotentialSkyline = true;
		}
		cleanItemMap.values.flatMap(x => x.instances).toList
	}

	def rule3(instList: List[Instance]) = {
		val wrTree = new WRTree(instList, "area");
		wrTree.run();
	}
}
