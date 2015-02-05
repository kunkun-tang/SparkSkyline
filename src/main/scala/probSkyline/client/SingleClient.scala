package probSkyline.client

import probSkyline.query._
import scala.collection.mutable.ListBuffer
import java.io.File
import probSkyline.query._
import probSkyline.Util
import com.typesafe.config.ConfigFactory

/**
 * SingleClient reads data from the folder part as the source,
 * and do pruning based on rectangle pruning, and write data to
 * .result file/
 */
object SingleClient extends App{

	if(args.length != 2){
		println("usage: scala SingleClient naive(optimized) single(all)")
		System.exit(1);
	}
	val argStr = args(1)

	if(args(0) == "naive"){
		val conf = ConfigFactory.load;
		val testArea = conf.getString("Query.testArea");
		val splitNum = conf.getInt("Query.splitNum");

		println("The naive computing strategy: ");
		if(argStr == "single"){
			val nqClient = new NaiveQuery(testArea)
			nqClient.compProb(nqClient.getItemList)
		}
		else{
			val nqClient = new NaiveQuery(testArea);
			for(i<- 0 until splitNum){
				nqClient.changeTestArea(i.toString)
				nqClient.compProb(nqClient.getItemList)
			}
		}
	}
	else if(args(0) == "medium"){
		val conf = ConfigFactory.load;
		val fileName = conf.getString("Query.srcFile");

		val mqClient = new MediumQuery(fileName);
		mqClient.compProb(mqClient.getItemList);
	}
	else{
		val conf = ConfigFactory.load;
		val testArea = conf.getString("Query.testArea");
		val splitNum = conf.getInt("Query.splitNum");

		println("The WRTree computing strategy: ");
		if(argStr == "single"){

			val oqClient = new OptimizedQuery(testArea, OptimizedQuery.getItemMap(testArea) );
			oqClient.readMAXMIN();
			oqClient.rule1();
			oqClient.rule2();
			oqClient.rule3();
		}
		else{
			for(i<- 0 until splitNum){
				val iStr = i.toString();
				val oqClient = new OptimizedQuery(iStr, OptimizedQuery.getItemMap(iStr) );
				oqClient.readMAXMIN();
				oqClient.rule1();
				oqClient.rule2();
			}
		}
	}

	/**
	 * get the files uner a folder recursively.
	 * Since part folder also has other files, the function filters files which are ".txt" files
	 */
	def listAllFiles(fList: ListBuffer[File], path:File){
		if(path.isDirectory()){
			for{f<-path.listFiles() if f.getName().substring(f.getName().length()-3) == "txt" }{
				if(f.isFile) fList += f.getAbsoluteFile() 
				else listAllFiles(fList, f)
			}
		}
	}
}
