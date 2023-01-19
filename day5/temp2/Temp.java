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

	public class Temp {

	public static class internetMapper extends Mapper<Object,Text,Text, FloatWritable>
	{
	public void map(Object key,Text value , Context context) throws IOException,InterruptedException
	{
	String line[]= value.toString().split("\\s+");
	float maxtemp = Float.parseFloat(line[5]);
	String year = line[1].substring(0,4);
	    
          if (  maxtemp > -60.0f && maxtemp < 60.0f )
          context.write(new Text (year), new FloatWritable(maxtemp));
  }
  }
   public static class internetReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	
	float globaltemp=0.0f;
	String year = null ;
	public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException,InterruptedException
	{
	float max=-100;
         for(FloatWritable temp : values)
            {
              if (temp.get() > max )
              max=temp.get();
        

            }
              if (max > globaltemp)
              {
              globaltemp=max;
              year=key.toString();
              }
             }
             public void cleanup(Context context) throws IOException,InterruptedException
             {
             context.write(new Text("Hottest year is=" +year), new FloatWritable(globaltemp));
        }
  }      
          
public static void main(String args[]) throws Exception
{
  Configuration conf=new Configuration();
  Job job=new Job(conf, "internet");
  

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(FloatWritable.class);
  

  job.setOutputFormatClass(TextOutputFormat.class);
  job.setInputFormatClass(TextInputFormat.class);
   

   job.setMapperClass(internetMapper.class);
   job.setReducerClass(internetReducer.class);
   

   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   

   //job.setMapOutputKeyClass(Text.class);
   //job.setMapOutputValueClass(FloatWritable.class);
   job.waitForCompletion(true);
   
  }
 }
   
