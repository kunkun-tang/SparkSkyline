import scala.collection.mutable.ListBuffer

import com.typesafe.config._
import scala.math;

object Config {
	
  private val root = ConfigFactory.load()
  val ClusterConfig = root.getConfig("ClusterConfig")
  val arrDouble = computeArrDouble()

  def computeArrDouble() = {
  	val conf = ConfigFactory.load;
		val dim = conf.getInt("GenData.dim");
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
//			for(i<-0 until splitNum/3 )
//				arr2.append(math.Pi/(splitNum/3)*(i+1)/2);	

			arr2.append(0.7854);	
			arr2.append(1.57079);	
			retList.append(arr2);
		}
		retList
	}
	
}
