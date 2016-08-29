package shubham;
import org.apache.hadoop.conf.Configuration;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import shubham.CriticalViolations.MyMapper;
import shubham.CriticalViolations.MyReducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountArea {
public static class MyMapper extends Mapper<LongWritable, Text,Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			String line = value.toString();
			String[] arr=line.split("\t");
			
			if(arr.length!=17 && arr.length!=18){
				System.out.println("EXEC" + arr.length + "  " + line);
			}
			
			context.write(new Text(arr[2]),new Text(line));
			
		}
	
}
public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		int critical=0;
		int nonC=0;
		int noV=0;
		
		for(Text t:values){
		
			String line =t.toString();
			String[] arr = line.split("\t");
			
			if(arr.length==17){
				noV++;
			}
			else if(arr[12].equals("Not Applicable")){
				
				noV++;
			}
			else if(arr[12].equals("Critical")){
				critical++;
			}
			else if(arr[12].equals("Not Critical")){
				nonC++;
			}
		
		}
		if(!key.toString().equals("BORO") && !key.toString().equals("Missing"))
		context.write(key, new Text("Critical Violation: " + critical + "  Non Critical Violations: " + nonC + "  No Violations: " + noV));
	}
}

public static void main(String[] args) throws Exception{
	Configuration conf= new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	Job job = new Job(conf, "Count Per Area");
	
	job.setJarByClass(CriticalViolations.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path (otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	System.exit(job.waitForCompletion(true)?0:1);
}


}
