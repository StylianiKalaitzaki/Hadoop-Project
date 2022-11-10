
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class UserDocParser
{
 
  public static class FirstMapper extends Mapper<Object, Text, Text, NullWritable>
  {
    private Text ipDocDate = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
      String docName;

      while (itr.hasMoreTokens()) 
	  {
		String[] data = itr.nextToken().split(",");
		docName=data[6];
		if(data[6].startsWith("."))
			docName=data[5]+data[6];
  	  	ipDocDate.set(data[0]+","+docName+","+data[1]);
		context.write(ipDocDate, NullWritable.get());

      }
    }
  }

  public static class FirstReducer extends Reducer<Text,NullWritable,Text,NullWritable> 
  {

    public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException 
	{
      context.write(key,NullWritable.get());
    }
    
  }
  
  public static class SecondMapper extends Mapper<Object, Text, Text, Text>
  {
    private Text ipDoc = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");

      while (itr.hasMoreTokens()) 
	  {
		String[] data = itr.nextToken().split(",");
  	  	ipDoc.set(data[0]+","+data[1]);
		context.write(ipDoc, new Text("1"));

      }
    }
  }
  
  public static class SecondReducer extends Reducer<Text,Text,Text,Text> 
  {
    private Text ip = new Text();
    private Text doc = new Text();

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
      int count = 0;
      for (Text val : values) 
	  {
        count += Integer.parseInt(val.toString());
      }
      if(count>1)
      {
    	  String[] data = key.toString().split(",");
    	  ip.set(data[0]);
    	  doc.set(data[1]);
    	  context.write(ip,doc);
      }
    	  
    }
    
  }
 
  public static void main (String[] args) throws Exception 
  {
	
	  if(args.length!=2 && args.length!=3)
	  {
		  System.out.println("Usage: UserDocParser.jar UserDocParser <input directory> <output direcoty> <number of reducers(optional)>");
		  System.exit(1);
	  }
	  
	  Configuration conf = new Configuration();
	  Job job1 = Job.getInstance(conf, "first_mapreduce");
    
    
	  int redTasks=1;
	  
	  job1.setJarByClass(UserDocParser.class);
	  
	  if(args.length==3)
		  redTasks=Integer.parseInt(args[2]);
	  job1.setNumReduceTasks(redTasks);
    
  
	  job1.setMapperClass(FirstMapper.class);
	  job1.setCombinerClass(FirstReducer.class);
	  job1.setReducerClass(FirstReducer.class);
    
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(NullWritable.class);
    
	  FileInputFormat.addInputPath(job1, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp"));
	  job1.waitForCompletion(true);
    
    
    
	  Configuration conf2 = new Configuration();
	  Job job2 = Job.getInstance(conf2, "second_mapreduce");
	  job2.setNumReduceTasks(redTasks);

    
	  job2.setJarByClass(UserDocParser.class);
	  job2.setMapperClass(SecondMapper.class);
	  job2.setReducerClass(SecondReducer.class);
	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);
    
	  //Defines the output of the first mapReduce as the input of the second mapReduce
	  FileInputFormat.addInputPath(job2, new Path(args[1]+ "/temp" ));
	  FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/final"));
    
   
	  int res=job2.waitForCompletion(true) ? 0 : 1;
    
	  //Deletes the temp directory created as the output of the first mapReduce
	  FileSystem fs = FileSystem.get(conf);
	  fs.delete(new Path(args[1]+ "/temp" ), true);
    
	  System.exit(res);
  }
}