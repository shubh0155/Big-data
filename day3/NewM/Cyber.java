import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Cyber
{
  public static class LogMapper extends MapReduceBase implements
  Mapper <Object ,/*Input key type*/
  Text, /*Input value type*/
  Text, /*Output key type*/
  FloatWritable> /*Output value type*/
  {
    //map function
    public void map(Object key, Text value,
    OutputCollector<Text, FloatWritable> output, 
    Reporter reporter) throws IOException
    {
      String line = value.toString();
      int add=0;
      StringTokenizer s=new StringTokenizer(line,"\t");
      String name=s.nextToken();
      while(s.hasMoreTokens())
      {
        add += (Integer.parseInt(s.nextToken()));
      }
      float avgtime = add/7.0f; //calc avg
      output.collect(new Text(name), new FloatWritable(avgtime));
      
    }
  }
  //Reducer class
  public static class LogReducer extends MapReduceBase implements
  Reducer <Text ,/*Input key type*/
  FloatWritable, /*Input value type*/
  Text, /*Output key type*/
  FloatWritable> /*Output value type*/
  {
    public void reduce(Text key, Iterator <FloatWritable> values,
    OutputCollector<Text, FloatWritable> output, 
    Reporter reporter) throws IOException
    {
      float val=0.0f;
      while (values.hasNext())
      {
        if((val=values.next().get())> 5.0f)
          output.collect(key, new FloatWritable(val));
      }
    }
  }

public static void main(String args[]) throws Exception
{
  // crate the object of configuration class
  JobConf conf=new JobConf(Cyber.class);
  
  //create a object class
  conf.setJobName("Internet Log");
  
    //set the data type of output key
  conf.setOutputKeyClass(Text.class);
  
    //set the data type of output value
  conf.setOutputValueClass(FloatWritable.class);
  
    //set the data format of output
  conf.setOutputFormat(TextOutputFormat.class);

        //set the data format of input
   conf.setInputFormat(TextInputFormat.class);
   
   //set the name of mapper class
  conf.setMapperClass(LogMapper.class);
   
   //set the name of REDUCER class
   conf.setReducerClass(LogReducer.class);
   
   //set the INPUT files path from 0th args
   FileInputFormat.setInputPaths(conf, new Path(args[0]));
   
   //set the output files path from 1st args
   FileOutputFormat.setOutputPath(conf, new Path(args[1]));

   JobClient.runJob(conf);
  }
}
