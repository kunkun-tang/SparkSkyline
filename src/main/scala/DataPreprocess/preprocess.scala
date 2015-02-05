package DataPreprocess

import com.typesafe.config.ConfigFactory
import org.json4s._
import org.json4s.native.JsonMethods._
// import org.json4s.JsonAST._
import org.json4s.native.JsonParser
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Preprocess
 */
object PreProcess extends App{

	val conf = ConfigFactory.load;
	val folderPath = conf.getString("GenData.folderPath");
	// val fileWriter = new FileWriter("real.txt", true);
	var objectID = 0;
	var instID = 0;
	val hotelMap = new scala.collection.mutable.HashMap[Int, String]();
	for(file <- new java.io.File(folderPath).listFiles){
		val fileName = file.getName();
		hotelMap += objectID -> fileName;


		/*
		 * The json format have one example as follows:
			
			""" {"Reviews": [
			{"Ratings": {"Service": "4", "Cleanliness": "5", "Overall": "5.0", "Value": "4", "Sleep Quality": "4", "Rooms": "5", "Location": "5"}, "AuthorLocation": "Boston", "Title": "\u201cExcellent Hotel & Location\u201d", "Author": "gowharr32", "ReviewID": "UR126946257", "Content": "We enjoyed the Best Western Pioneer Square. My husband and I had a room with a king bed and it was clean, quiet, and attractive. Our sons were in a room with twin beds. Their room was in the corner on the main street and they said it was a little noisier and the neon light shone in. But later hotels on the trip made them appreciate this one more. We loved the old wood center staircase. Breakfast was included and everyone was happy with waffles, toast, cereal, and an egg meal. Location was great. We could walk to shops and restaurants as well as transportation. Pike Market was a reasonable walk. We enjoyed the nearby Gold Rush Museum. Very, very happy with our stay. Staff was helpful and knowledgeable.", "Date": "March 29, 2012"}, 
			{"Ratings": {"Overall": "5.0"}, "AuthorLocation": "Madison, Wisconsin", "Title": "\u201cGreat Visit to Seattle!\u201d", "Author": "Nancy W", "ReviewID": "UR126795011", "Content": "Great visit to Seattle thanks to our stay at the Best Western Pioneer Square! The hotel was reasonably priced and close to everything we wanted to see - ferry ride, Underground Tour, Klondike Museum, short walk to Pike Market and other shopping. The staff was amazingly helpful and accommodating. Our room was very clean and had everything we needed. Breakfast was plentiful and very good. Before we booked, I read about some potential issues with the area. I can honestly say that the area was just fine! In fact, if you enjoy historic and quaint parts of town, this is definitely where you want to stay. I will be recommending this hotel to anyone who is headed to Seattle.", "Date": "March 27, 2012"}
			]}"""
		 */
		val json = parse(file);

		/*
		 * Retrieve Ratings from original json, and
		 * make it as a list.
		 */
		val js = for {
	    JObject(review) <- json \ "Reviews"
	    JField("Ratings", JObject(ratings)) <- review if ratings.length == 8
	  } yield ratings

	  // transform element from JDouble to Double
	  var valMatrix = js.map(list => list.map{ case (str, aVal) => 
	  	aVal match{
	  		case JString(x) => if(x.toDouble > 0.0)6.0 - x.toDouble else x.toDouble
	  		case _ => throw new Exception("only JString works.")
	  	}
	  })
	  
	  //ignore negative number from element.
	  valMatrix = valMatrix.filter{ elem => ifAllNumPos(elem) == true};

	  if(valMatrix.length > 0){

	  	store(valMatrix)
			objectID += 1
		}
		// println(valMatrix)
		if(objectID %100 == 0) println(objectID)
	}

	//check if all elements of a list are all positive.
	def ifAllNumPos(list: List[Double])={
		var len = 0;
		list.foreach(elem => if(elem > 0) len += 1)
		len == list.length
	}

	def store(valMatrix: List[List[Double]]) = {

		// println("len = "+valMatrix.length);
		val probList = randomFixedSum(valMatrix.length);

		val instAndProb = valMatrix.zip(probList);

		printToFile(new FileWriter("real.txt", true))(p => {
			instAndProb.foreach(x => {p.println(objectID + " " + instID + " "+ x._1.mkString(" ") + " " + x._2);
														 instID += 1;});
		})
	}

	def printToFile(f: java.io.FileWriter)(op: java.io.PrintWriter => Unit) {
	  val p = new java.io.PrintWriter(f)
	  try { op(p) } finally { p.close() }
	}

	import org.uncommons.maths.random.GaussianGenerator;
	import org.uncommons.maths.random.MersenneTwisterRNG;
	import java.util.Random;
	import org.uncommons.maths.random.ContinuousUniformGenerator;

	/*
	 * Randomly generate instNum numbers, all of which is sum to 1.
	 */
	def randomFixedSum(instNum: Int) = {
		val rng = new MersenneTwisterRNG();
		val rfs = new ContinuousUniformGenerator(0.0, 1.0, rng);
		var ret = Array.fill[Double](instNum)(rfs.nextValue());
		ret(0) = 0.0;
		scala.util.Sorting.quickSort(ret);
		for(i<- 0 until instNum-1)
			ret(i) = ret(i+1) - ret(i);
		ret(instNum-1) = 1- ret(instNum-1)
		ret
	}

}
