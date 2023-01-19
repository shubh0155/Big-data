import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

public class links {
	public static class linkMapper extends Mapper<Object,Text,Text, IntWritable>{
	boolean flag=false;

	public void map(Object key , Text value , Context context) throws IOException,InterruptedException
	{
	String line = value.toString();
	  if(flag) {
	 
	StringTokenizer s=new StringTokenizer(line,",");
	String id = s.nextToken();
	String type = s.nextToken();
	if(type.equals("link"))
	{
	String date = s.nextToken();
	int add = 0;
	while(s.hasMoreTokens())
	{   int num = Integer.parseInt(s.nextToken());
	    add+= num;
        }
         context.write(new Text(date) , new IntWritable(add));
       }
        }
        flag=true;
        }
  }
    
public static void main(String args[]) throws Exception
{
  // crate the object of configuration class
  Configuration conf=new Configuration();
  
  //create a object class
  Job job=new Job(conf, "links");
  
    //set the data type of output key
  job.setOutputKeyClass(Text.class);
  
    //set the data type of output value
  job.setOutputValueClass(IntWritable.class);
  
    //set the data format of output
  job.setOutputFormatClass(TextOutputFormat.class);

        //set the data format of input
   job.setInputFormatClass(TextInputFormat.class);
   
   //set the name of mapper class
   job.setMapperClass(linkMapper.class);
   
   job.setNumReduceTasks(0);
   
   //set the INPUT files path from 0th args
   FileInputFormat.addInputPath(job, new Path(args[0]));
   
   //set the output files path from 1st args
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   //Execute the job and wait for completion
   job.waitForCompletion(true);
  }
 }
   
   
