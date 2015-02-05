package probSkyline.genData

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory

object SplitData extends App{
	

	val conf = ConfigFactory.load;
	val fileName = conf.getString("GenData.srcFile");
	val dim = conf.getInt("GenData.dim");
	val objectNum = conf.getInt("GenData.objectNum");
	val splitNum = conf.getInt("GenData.splitNum")

	/*
	 * read original data file's data to memory.
	 * from file to itemList, which represents the list of items.
	 */
	var itemList = Util.getItemList(fileName)
	println("The size of items = "+ itemList.length)
	/*
	 * write instance to each file based on angle partitioning scheme.
	 * first, we create files, each of which corresponds a partition.
	 */
	val TIWs = new Array[InstanceWriter](splitNum)
	for(i<-0 until splitNum)
		TIWs(i) = new InstanceWriter(i.toString, "part")

	//Based on the partition return result, we put instance to that file.:w
	for(aItem <- itemList){
		for(aInst <- aItem.instances){
			val ret_area = Util.getPartition(aInst);
			TIWs(ret_area).writeInstance(aInst);
		}
	}

	// after writing finishes, it should close the file.
	for(i<-0 until splitNum)
		TIWs(i).close()


	/*
	 * find left bottom and right top extreme value of every item, and add to outputList.
	 * first, we create files, each of which corresponds a partition.
	 */

	var outputLists = new ListBuffer[PartitionInfo]();
	for(i<-0 until splitNum)
		outputLists.append(new PartitionInfo(i))

	/*
	 * find every item's max and min point through findExteme method.
	 */
	for(aItem <- itemList) findExtreme(aItem, outputLists)
	
	import java.io.FileOutputStream;
	import java.io.ObjectOutputStream;
	try{

		val fileOut = new FileOutputStream( "./part/MAX_MIN" );
		var outStream = new ObjectOutputStream(fileOut);
		outStream.writeObject(outputLists);

		outStream.flush();
		outStream.close();
		fileOut.close();

	}catch{case e: IOException =>  e.printStackTrace(); }



	/*
	 * the function to compute the min and max extreme value of
	 * every object.
	 */
	def findExtreme(aItem: Item, outputList: ListBuffer[PartitionInfo]){

		val areaSet = new HashSet[Int]
		var min= new Point(SplitData.dim)
		min.setOneValue(1.0)
		var max= new Point(SplitData.dim)
		max.setOneValue(0.0)

		for(aInstance <- aItem.instances)
			for(j<-0 until SplitData.dim){

				if(aInstance.pt(j) < min(j))
					min.coordinates(j) = aInstance.pt(j);

				if(aInstance.pt(j) > max(j))
					max.coordinates(j) = aInstance.pt(j);
			}

		for(aInst <- aItem.instances){
			val area = Util.getPartition(aInst);
			if(!areaSet.contains(area)){

				val aPartInfo = outputList(area);
				aPartInfo.addMax(aItem.objID, max);
				aPartInfo.addMin(aItem.objID, min);
				areaSet.add(area)
			}
		}
	}

}
