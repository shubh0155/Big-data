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

public class avgty {

	public static class marksMapper extends Mapper<Object,Text,Text, IntWritable>
	{
	
	public void map(Object key , Text value , Context context) throws IOException,InterruptedException
	{
	String line = value.toString();
	StringTokenizer st = new StringTokenizer(line,",");
	String name = st.nextToken();
	int marks =Integer.parseInt(st.nextToken());
	String cls = st.nextToken();
	 if(cls.equals("ty"))

            context.write(new Text("marks"), new IntWritable(marks));
      }      
        
  }
  public static class marksReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
	{
	FloatWritable avg = new FloatWritable();
	int sum=0;
	int b=0;

	for(IntWritable num : values) 
            {
            sum=sum+num.get();
            b++;
        }
        
          avg.set((float)sum/b);
            
        
        context.write(new Text ("Average is"), avg);
    }
}
        
          
public static void main(String args[]) throws Exception
{
  // crate the object of configuration class
  Configuration conf=new Configuration();
  
  //create a object class
  Job job=new Job(conf, "age");
  
    //set the data type of output key
  job.setOutputKeyClass(Text.class);
  
    //set the data type of output value
  job.setOutputValueClass(FloatWritable.class);
  
    //set the data format of output
  job.setOutputFormatClass(TextOutputFormat.class);

        //set the data format of input
   job.setInputFormatClass(TextInputFormat.class);
   
   //set the name of mapper class
   job.setMapperClass(marksMapper.class);
   
   //set the name of REDUCER class
   job.setReducerClass(marksReducer.class);
   
   //set the INPUT files path from 0th args
   FileInputFormat.addInputPath(job, new Path(args[0]));
   
   //set the output files path from 1st args
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   //set the data type of output key
   job.setMapOutputKeyClass(Text.class);
   
   //set the data type of output value
   job.setMapOutputValueClass(IntWritable.class);
   
   //Execute the job and wait for completion
   job.waitForCompletion(true);
  }
 }
   
   
