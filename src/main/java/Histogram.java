
import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable<Color> {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
	public String toString(){
	return String.valueOf(this.type)+" "+String.valueOf(this.intensity);
	}

	public Color(short type, short intensity){
	this.type = type;
	this.intensity = intensity;
	}

	public Color() {
	this.type = type;
	this.intensity = intensity;
	}
	
	public void write(DataOutput out) throws IOException {
         out.writeShort(type);
         out.writeShort(intensity);
       }
       
       public void readFields(DataInput in) throws IOException {
         type = in.readShort();
         intensity = in.readShort();
       }
	
	public int compareTo(Color o){ 
      
	if((this.type == o.type) && (this.intensity == o.intensity)){
	return 0;
	}
	if((this.type < o.type)){
	return -1;
	}
	else if(this.type == o.type){
		if(this.intensity < o.intensity) {return -1;}
		else{return 1;}
	}
	else{
	return 1;
	}
       
  }


       	
       public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + type;
         result = prime * result + (int) (intensity ^ (intensity >>> 32));
         return result;
       }
	
	
}


public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
		Scanner s = new Scanner(value.toString()).useDelimiter(",");
		short red = s.nextShort();
		short green = s.nextShort();
		short blue = s.nextShort();

		context.write((new Color((short)1,red)), new IntWritable(1));
		context.write((new Color((short)2,green)), new IntWritable(1));
		context.write((new Color((short)3,blue)), new IntWritable(1));

		

        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
		long sum = 0;
		for (IntWritable v: values){
		sum+= v.get();
		}
		context.write(key, new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
	  Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
   }
}
