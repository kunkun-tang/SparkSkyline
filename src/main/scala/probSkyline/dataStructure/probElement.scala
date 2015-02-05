package probSkyline.dataStructure;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.HashMap;
import java.io.DataInput;
import java.io.DataOutput;

class Instance(val objID: Int, val instID: Int, val prob: Double, val dim: Int)extends Serializable{ 
	var instSkyProb = 0.0
	var pt: Point = new Point(dim)
	var instPotentialSkyline = true;

	def checkDomination(other: Instance) = pt.checkDomination(other.pt)

	def this(onePoint: Point){
		this(0, 0, 0.0, onePoint.dim)
		this.pt.setPoint (onePoint)
	}

	def setPoint(arr: Array[Double]){
		pt.setArrValue(arr)
	}

	def sum = pt.sum

	override def toString = objID.toString + " " + instID.toString + " " + pt.toString +  " " + prob.toString

}


class Item(val objID: Int) extends Serializable{
	
	var objSkyProb = 0.0
	var potentialSkyline = true;
	val instances = ListBuffer[Instance]()
	var min: Point = null
	var max: Point = null

	def addInstance(inst: Instance){
		instances += inst
	}

	def setMin(aMin: Point){
		min = aMin
	}
	
	def setMax(aMax: Point){
		max = aMax
	}

	override def toString = {
		val strBuilder = new scala.collection.mutable.StringBuilder()
		for(i <- instances){
			strBuilder ++= i.toString + "\n";
		}
		strBuilder.toString
	}

}

class PartitionInfo(val splitNo: Int) extends Serializable{


	val min = HashMap[Int, Point]();
	val max = HashMap[Int, Point]();

	def addMin(objID: Int, onePoint: Point){
		min.update(objID,onePoint);
	}

	def addMax(objID: Int, onePoint: Point){
		max.update(objID,onePoint);
	}
}
