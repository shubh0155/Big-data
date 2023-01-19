import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.StringTokenizer;
import java.io.IOException;

	public class FBS {

	public static class internetMapper extends Mapper<Object,Text,IntWritable, IntWritable>
	{
	public void map(Object key,Text value , Context context) throws IOException,InterruptedException
	{
	String line[]= value.toString().split("\\s+");
	String date = line[2];
	int temp = (Integer.parseInt(line[5]));
	    
          if (  temp<(50) && temp >(-50) )
          
          {
            date = date.substring(0,3);
            context.write(new IntWritable(Integer.parseInt(date)), new IntWritable(temp)); 
            
          }
  }
  }
   public static class internetReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
	{
	IntWritable average = new IntWritable();
		
		int max = 0;
       for(IntWritable num : values)
            {
              if (num.get() > max )
              max =num.get();

            }
         
            average.set(max);
        context.write(key,average);
    }
}
       
          
public static void main(String args[]) throws Exception
{
  // crate the object of configuration class
  Configuration conf=new Configuration();
  
  //create a object class
  Job job=new Job(conf, "internet");
  
    //set the data type of output key
  job.setOutputKeyClass(IntWritable.class);
  
    //set the data type of output value
  job.setOutputValueClass(IntWritable.class);
  
    //set the data format of output
  job.setOutputFormatClass(TextOutputFormat.class);

        //set the data format of input
   job.setInputFormatClass(TextInputFormat.class);
   
   //set the name of mapper class
   job.setMapperClass(internetMapper.class);
   
   //set the name of REDUCER class
   job.setReducerClass(internetReducer.class);
   
   //set the INPUT files path from 0th args
   FileInputFormat.addInputPath(job, new Path(args[0]));
   
   //set the output files path from 1st args
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   //set the data type of output key
   job.setMapOutputKeyClass(IntWritable.class);
   
   //set the data type of output value
   job.setMapOutputValueClass(IntWritable.class);
   
   //Execute the job and wait for completion
   job.waitForCompletion(true);
  }
 }
   
