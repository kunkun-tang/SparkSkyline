package probSkyline.dataStructure
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io._;


class PointWritable(var valueClass: java.lang.Class[DoubleWritable], var values: Array[Writable]) extends ArrayWritable(valueClass, values){

	def this() = this(classOf[DoubleWritable], null);

}

class InstanceWritable(var objID: IntWritable, var instID: IntWritable, var prob: DoubleWritable, var pw: PointWritable) extends Writable{
	
	def this() = this( new IntWritable(),  new IntWritable(), new DoubleWritable(), new PointWritable() );

	def readFields(in: DataInput){
		objID.readFields(in);
		instID.readFields(in);
		prob.readFields(in);
		pw.readFields(in);
	}

	def write(out: DataOutput){
		objID.write(out);
		instID.write(out);
		prob.write(out);
		pw.write(out);
	}


}

