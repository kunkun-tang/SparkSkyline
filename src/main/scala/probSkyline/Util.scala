package probSkyline

import scala.collection.mutable.ListBuffer
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.math;

import probSkyline.dataStructure._
import com.typesafe.config.ConfigFactory

object Util{

	val conf = ConfigFactory.load;
	val srcName = conf.getString("Query.srcFile");
	val dim = conf.getInt("Query.dim");
	val objectNum = conf.getInt("Query.objectNum");

	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	def getItemList(fileName: String) = {
		var aMap = new HashMap[Integer, Item]();
		for(line <- Source.fromFile(fileName).getLines()){

			val curr = stringToInstance(line);
			if(!aMap.contains(curr.objID)){
				val aItem = new Item(curr.objID);              
				aMap.update(curr.objID, aItem);
			}
			aMap(curr.objID).addInstance(curr);
		}
		val itemList = aMap.values.toList
		itemList
	}

	import org.apache.spark.rdd.RDD
	def getItemListFromRDD(iterInst: RDD[Instance]) = {
		var aMap = new HashMap[Integer, Item]();
		for(curr <- iterInst){

			if(!aMap.contains(curr.objID)){
				val aItem = new Item(curr.objID);              
				aMap.update(curr.objID, aItem);
			}
			aMap(curr.objID).addInstance(curr);
		}
		val itemList = aMap.values.toList
		itemList
	}


	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	def getItemMap(fileName: String) = {

		var aMap = new HashMap[Integer, Item]();
		for(line <- Source.fromFile(fileName).getLines()){

			val curr = stringToInstance(line);
			if(!aMap.contains(curr.objID)){
				val aItem = new Item(curr.objID);              
				aMap.update(curr.objID, aItem);
			}
			aMap(curr.objID).addInstance(curr);
		}
		aMap
	}

	def getItemMapFromIterable(iterInst: Iterable[Instance]) = {

		var aMap = new HashMap[Integer, Item]();
		for(curr <- iterInst){

			if(!aMap.contains(curr.objID)){
				val aItem = new Item(curr.objID);              
				aMap.update(curr.objID, aItem);
			}
			aMap(curr.objID).addInstance(curr);
		}
		aMap
	}

	/*
	 * given a string, stringToInstance creates an instance.
	 */
	def stringToInstance(line: String) = {
		val div = line.split(" ");
		var inst: Instance = null
		if(div.length == dim + 3){
			inst = new Instance(div(0).toInt, div(1).toInt, div(div.length-1).toDouble, dim);
			inst.setPoint(getPoint(div))
		}
		else println("Sth wrong in StringToInstance in Util.")
		inst
	}


	def getPoint(div: Array[String]) = {
		var arrRet = new Array[Double](dim)
		for(i<-0 until dim) arrRet(i) = div(i+2).toDouble
		arrRet
	}


	class GenQuantilePartition(val total: Int, var numPart: Int) extends Serializable{
		import scala.collection.mutable.ListBuffer;
		
		/*
		 * if the total is 7, numPart is 4.
		 * the list will be (1, 2, 2, 2)
		 */
		val partList = {
			var totalCopy = total;
			var list = new ListBuffer[Int]();
			while(totalCopy > 0 && numPart > 0){
				val curr: Int = totalCopy/numPart;
				list += curr;
				totalCopy -= curr;
				numPart -= 1;
			}
			list.scanLeft(0)(_ + _).drop(1)
		}

		def genPart(index: Int) = {
			// println("index = "+index + "  total = "+total)
			if(index >= total) throw new Exception("index can not be larger then total number")
			var part = 0;
			while(index >= partList(part)){
				part += 1
			}
			part
		}
	}

	/*
	 * Given an instance, getPartition finds the partition number for the 
	 * instance.
	 */
	def getPartition(aInst: Instance) = {
		var ret = -1;
		if(aInst != null){

			/*
			 * compute the length of the radius firstly.
			 */
			var r = 0.0;
			for(i<- 0 until dim)
				r += aInst.pt(i) * aInst.pt(i);

			/*
			 * Detailed computation procedure could be referred in Angle-based Space Partitioning for Efficient Parallel
			 * Skyline Computation.
			 */
			var angle = new Array[Double](dim-1)
			for(i<-0 until angle.length){
				r -= aInst.pt(i) * aInst.pt(i);
				val tanPhi = math.sqrt(r)/aInst.pt(i);
				angle(i) = math.atan(tanPhi);	
			}

			val arrDouble = computeArrDouble();

			/* 
			 * Current partitioning scheme only supports two and three dimensional cases.
			 * if the returned value is -1, it denotes that sth wrong happened in the get partition number Process.
			 */
			if(dim == 2){
				val angles = arrDouble(0);
				for(i<-(0 until angles.length).reverse)
				  if (angles(i) > angle(0)) ret = i;
			}
			else if(dim == 3){

				val anglesX = arrDouble(0);
				var partitionX = -1;

				for{i<- anglesX.length-1 to (0, -1); if anglesX(i) > angle(0)}
						partitionX = i;

				val anglesY = arrDouble(1);
				var partitionY = -1;
				for{i<- anglesY.length-1 to (0, -1); if anglesY(i) > angle(1) }{
						partitionY = i;
				}

				if(partitionX == -1 || partitionY == -1)
					ret = -1;
				else{
					ret = partitionY * anglesX.length + partitionX;
				}
			}
		}
		ret;
	}

  def computeArrDouble() = {
		val splitNum = conf.getInt("GenData.splitNum");

		val retList = new ListBuffer[ListBuffer[Double]](); 
		if(dim == 2){
			val arr = new ListBuffer[Double](); 
			for(i<-0 until splitNum) 
				arr.append(math.Pi/2/splitNum*(i+1));
			
			retList.append(arr);
		}
		else if (dim == 3){
			val arr = new ListBuffer[Double]();
			// for(i<-0 until 3) 
			// 	arr.append(math.Pi/3*(i+1)/2);	

//			arr.append(0.9235);
//			arr.append(1.04);
			arr.append(1.57079);

			retList.append(arr);

			val arr2 = new ListBuffer[Double]();
			arr2.append(0.7854);	
			arr2.append(1.57079);	
			retList.append(arr2);
		}
		retList
	}

import java.lang.Object;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

	def serialize(obj: Object) = {
    var ret: Array[Byte] = null;
    try{
    	val out = new ByteArrayOutputStream();
    	val os = new ObjectOutputStream(out);
    	os.writeObject(obj);
    	ret = out.toByteArray();
    }catch{
    	case e:Exception => e.printStackTrace();
    }
    ret;
	}

	def deserialize(data: Array[Byte]) = {
	  var obj: Object = null;
	  try{

		  val in = new ByteArrayInputStream(data);
		  val is = new ObjectInputStream(in);
	  	obj = is.readObject();
	  }catch{
	  	case e:Exception => e.printStackTrace();
	  }
	  obj;
	}


}
