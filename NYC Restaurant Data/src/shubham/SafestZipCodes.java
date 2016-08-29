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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



import java.io.IOException;
import java.util.StringTokenizer;
public class SafestZipCodes {

public static class MyMapper extends Mapper<LongWritable, Text,Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			String line = value.toString();
			String[] arr=line.split("\t");
			
			if(arr.length!=17 && arr.length!=18){
				System.out.println("EXEC" + arr.length + "  " + line);
			}
			if(!arr[2].equals("BORO") && !arr[2].equals("Missing"))
			context.write(new Text(arr[2]),new Text(line));
			
		}
	
}

public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	
		ArrayList<Integer> list = new ArrayList<Integer>();
		int[] store = new int[100000];
		for(Text val: values){
			String[] arr = val.toString().split("\t");
			String zipcode = arr[5];
			if(arr[12].equals("Critical")){
				int code = Integer.parseInt(zipcode);
				int find = contains(list,code);
				if(find==-1){
					list.add(code);
					list.add(1);
				}
				else{
					int freq=list.get(find+1);
					list.set(find+1, freq+1);
				}
			}
		}
	
		int min=Integer.MAX_VALUE;
		int storeCode=0;
		int i=1;
		int sum=0;
		while(i<list.size()){
			int num=list.get(i);
			sum+=num;
		//	System.out.println(num);
			if(num<min && num!=0){
				min=num;
				storeCode=list.get(i-1);
			}
			i=i+2;
		}
	//	System.out.println(key.toString() + "   " + sum);
		context.write(key, new Text(storeCode+"   Number of restaurants with critical violations: " + min));
	}
	
	private int contains(ArrayList<Integer> list, int code){
		int i=0;
		while(i<list.size()){
			if(list.get(i)==code)
				return i;
			else
				i=i+2;
		}
		return -1;
	}
}

public static void main(String[] args) throws Exception{
	Configuration conf= new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	Job job = new Job(conf, "Safest Zip Codes Per Area");
	
	job.setJarByClass(SafestZipCodes.class);
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
