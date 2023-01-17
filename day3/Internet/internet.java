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

public class internet {

	public static class internetMapper extends Mapper<Object,Text,Text, IntWritable>
	{
	
	public void map(Object key , Text value , Context context) throws IOException,InterruptedException
	{
	String line = value.toString();
	StringTokenizer st = new StringTokenizer(line,"\t");
	int sum = 0;
	while (st.hasMoreTokens())

	{
	String name = st.nextToken();
	for(int i=0;i<7;i++)
	{
	int d =Integer.parseInt(st.nextToken());
	 sum=sum+ d;
	}
	
          
            context.write(new Text(name), new IntWritable(sum)); 
            }
          }  
        
  }
   public static class internetReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
	{
	FloatWritable avg = new FloatWritable();
		int a= 0;
		
       for(IntWritable num : values) 
            {
                a=num.get();
        }

	float c = (float) a /7;
	if (c > 5 )
            
            {
        avg.set(c);
        context.write(key,avg);
    }}
}
       
          
public static void main(String args[]) throws Exception
{
  // crate the object of configuration class
  Configuration conf=new Configuration();
  
  //create a object class
  Job job=new Job(conf, "internet");
  
    //set the data type of output key
  job.setOutputKeyClass(Text.class);
  
    //set the data type of output value
  job.setOutputValueClass(FloatWritable.class);
  
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
   job.setMapOutputKeyClass(Text.class);
   
   //set the data type of output value
   job.setMapOutputValueClass(IntWritable.class);
   
   //Execute the job and wait for completion
   job.waitForCompletion(true);
  }
 }
   
   
