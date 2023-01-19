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

public class funny 
{
	public static class linkMapper extends Mapper<Object,Text,Text, IntWritable>
	{
	boolean flag=false;
	int max=0;
	String type = null;
	int rownum=0;
	public void map(Object key , Text value , Context context) throws IOException,InterruptedException
	{
	  String line[] = value.toString().split(",",12);
	  if(flag) 
	  {
	    rownum++;
	    if(Integer.parseInt(line[9]) > max)
	    {
	      max = Integer.parseInt(line[9]);
	      type = rownum + "\t" + line[1] + "\t" + line[2];
	    }
	  }
          flag=true;
        }
        // cleanup called once at the end of Mapper
        public void cleanup (Context context)
        throws IOException,InterruptedException {
          context.write(new Text (type), new IntWritable(max));
        }
  }
    
public static void main(String args[]) throws Exception
{
  
  Configuration conf=new Configuration();
  Job job=new Job(conf, "funny");
  

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  

  job.setOutputFormatClass(TextOutputFormat.class);
  job.setInputFormatClass(TextInputFormat.class);
  

  job.setMapperClass(linkMapper.class);
  job.setNumReduceTasks(0);
   

  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  job.waitForCompletion(true);
  }
  
 }
   
   
