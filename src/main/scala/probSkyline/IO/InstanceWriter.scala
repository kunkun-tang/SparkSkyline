package probSkyline.IO 
import scala.collection.mutable.ListBuffer
import probSkyline.dataStructure._;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


class InstanceWriter(val fileName: String, val folderName: String){
	
	def this(fileName: String){
		this(fileName, "")
	}

	def outputFile = {
		if(folderName == "") fileName;
		else{
			val folder = new File(folderName);
			if(!folder.exists()) folder.mkdir();
			"./" + folderName + "/" + fileName;
		}
	}

	println("output File = " + outputFile)
	val bw = new BufferedWriter(new FileWriter(outputFile));

  def write(a_string: String){
  	try{
      bw.write(a_string + "\n");
    }catch{
			case ioe: IOException => ioe.printStackTrace()	
		}
  }
    
  def writeInstance(a_inst: Instance){
      this.write(a_inst.toString);
  }  

	def close(){
		try{
			bw.flush();
			bw.close();
    }catch{
			case ioe: IOException => ioe.printStackTrace()	
		}
	}
}