package shubham;

import org.apache.hadoop.conf.Configuration;



import java.util.ArrayList;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.StringTokenizer;



public class CriticalViolations {
	public static class MyMapper extends Mapper<LongWritable, Text,Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line=value.toString();
			String[] arr=line.split("\t");
		
		String s1=arr[12];
		if(s1.equals("Critical")){
			String x=arr[1]+" ("+arr[3]+","+arr[4]+","+arr[2]+")";
			String small="";
			String shorten=arr[11];
			for(int i=0;i<shorten.length();i++){
				small+=shorten.charAt(i);
				if(shorten.charAt(i)=='.')
					break;
			}
			
			context.write(new Text(x),new Text(small));
			
		}
		
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text text: values){
				context.write(key, text);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf= new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = new Job(conf, "CriticalViolations");
		
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


