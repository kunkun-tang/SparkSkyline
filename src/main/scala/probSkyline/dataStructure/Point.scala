package probSkyline.dataStructure
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io._;


class Point(val dim: Int) extends Serializable{
	var coordinates = new Array[Double](dim)

	def this() = this(2)

	def setOneValue(aValue: Double){
		for(i<- 0 until dim) coordinates(i) = aValue
	}

	def setPoint(pt: Point){
		for(i<- 0 until dim) coordinates(i) = pt(i)
	}


	def setOneValue(onePoint: Point, ratio: Double){
		for(i<- 0 until dim) coordinates(i) = onePoint(i) * ratio
	}

	def setArrValue(arrDouble: Array[Double]){
		for(i<- 0 until dim) coordinates(i) = arrDouble(i)
	}

	def apply(i: Int) = coordinates(i)

	def setOffsetValue(onePoint: Point, currDim: Int, offset: Double){
		coordinates(currDim) = onePoint(currDim) + offset
	}

	def checkDomination(onePoint: Point) = {
		var ret = true;
		for(i<- 0 until dim if coordinates(i) >= onePoint(i) ) ret = false; 
		ret;
	}

	def sum() = coordinates.sum

	/*
	 * the function is in TODO List.
	 */
	def partition(splitVal: Double) = 0

	override def equals(obj: Any) = {
		obj	match{
			case p: Point => this.compareTo(p) == 0	
			case _ => false
		}
	}

	def compareTo(onePoint: Point) ={
		if(this.checkDomination(onePoint)) 1
		else if(this.checkDomination(onePoint)) -1
		else 0
	}

	override def toString = coordinates.mkString(" ")
}

