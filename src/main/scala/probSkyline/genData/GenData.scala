package probSkyline.genData

import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;
import java.util.Random;
import org.uncommons.maths.random.ContinuousUniformGenerator;

import probSkyline.Util;
import probSkyline.dataStructure._;
import probSkyline.IO._

import java.io.IOException;
import scala.collection.mutable.HashSet;
import scala.collection.mutable.ListBuffer;
import scala.io.Source;
import scala.math;
import com.typesafe.config.ConfigFactory

/*
 * After the raw data is obtained from the point distribution, we are able to 
 * generate instances using some random tools.
 */
object GenData extends App{
	
	val conf = ConfigFactory.load;
	val fileName = conf.getString("GenData.rawFile");
	val dim = conf.getInt("GenData.dim");
	val objectNum = conf.getInt("GenData.objectNum");
	val numInstOneObj = conf.getInt("GenData.instNum");
	val widthObj = conf.getDouble("GenData.widthObj");

	/*
	 * Use Instance writer to write instances.
	 */
	val TIW = new InstanceWriter(fileName + ".txt")
	var instID = 0;
	var objID = 0;

	/*
	 * Some random generator is created for further use.
	 */
	val rng = new MersenneTwisterRNG();
	val CUG = new ContinuousUniformGenerator(0.05, 0.95, rng);
	val GG = new GaussianGenerator(widthObj, 0.00625, rng);
	val CUFG = new ContinuousUniformGenerator(1, numInstOneObj, rng);

	def strToDouble(str: String) = str.toDouble;
	for(line <- Source.fromFile(fileName).getLines()){

		val center = new Point(dim);
		center.setArrValue( line.split(" ").map(e => strToDouble(e)) );
		val instNum = math.round(CUFG.nextValue()).toInt;
		val edgeWidth = GG.nextValue();
		val prob: Array[Double] = randomFixedSum(instNum);

		for(i <- 0 until instNum){
			val aInst = new Instance(objID, instID, prob(i), dim);
			aInst.setPoint(genRandomPoint(center,edgeWidth/2));
			TIW.writeInstance(aInst);
			instID += 1;
		}
		objID += 1;
	}



	// After writing finishes, it should close the file.
	TIW.close()

	/*
	 * Based on the center of a point, it generate random point with Gaussian distribution
	 */
	def genRandomPoint(center: Point, halfEdgeWidth: Double) = {
		val pt = new Array[Double](GenData.dim);
		val instDimGen = new ContinuousUniformGenerator(halfEdgeWidth*(-1), halfEdgeWidth, GenData.rng);

		for(i<-0 until GenData.dim){
			var aValue = instDimGen.nextValue();
			while(center(i) + aValue <=0.0 ||  center(i) + aValue >= 1.0){
				aValue = instDimGen.nextValue();
			}
			pt(i) = center(i) + aValue;
		}
		pt
	}


	/*
	 * Randomly generate instNum numbers, all of which is sum to 1.
	 */
	def randomFixedSum(instNum: Int) = {
		val rfs = new ContinuousUniformGenerator(0.0, 1.0, GenData.rng);
		var ret = Array.fill[Double](instNum)(rfs.nextValue());
		ret(0) = 0.0;
		scala.util.Sorting.quickSort(ret);
		for(i<- 0 until instNum-1) 
			ret(i) = ret(i+1) - ret(i);
		ret(instNum-1) = 1- ret(instNum-1)
		ret
	}

}
