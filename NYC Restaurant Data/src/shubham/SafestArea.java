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

import shubham.SafestZipCodes.MyMapper;
import shubham.SafestZipCodes.MyReducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class SafestArea {

//	final int BRONX = 110; //km squared
//	final int BROOKLYN = 180; //km squared
//	final int MANHATTAN = 59; //km squared
//	final int QUEENS = 280; //km squared
//	final int STATEN_ISLAND = 150; //km squared
	
	
public static class MyMapper extends Mapper<LongWritable, Text,Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line=value.toString();
			//System.out.println("line" + " " + line);
			int flag=0;
			String store ="";
			String s="";
			//String loc="";
		//	int flag2=0;
			for(int i=0;i<line.length();i++){
			//	System.out.println("EXECUTION");
			//	if(line.charAt(i)==' '){
			//		flag2=1;
			//	}
			//	else if(flag2==0){
			///		loc+=line.charAt(i);
			//	}
				int a=line.charAt(i);
				if(line.charAt(i)==':' && flag==0){
					flag = 1;
				}
				else if(line.charAt(i)==':' && flag==1){
					flag = 0;
				//	System.out.println("s" + s);
					store+=s+" ";
					
					s="";
			//		System.out.println("s  " + s + " " + store);
					flag=1;
				}
				else if(a>=48 && a<=57){
					s+=line.charAt(i);
				}
			}
			
			store+=s;
		//	System.out.println(store);
			//System.out.println(line.length());
			String[] arr = line.split("\t");
			System.out.println(arr[0]+" "+store +"print1");
			context.write(new Text("Area"), new Text(arr[0] + " " +store ));
		}
}

public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		String area="";
		double min = Double.MAX_VALUE;
		for(Text val:values){
		System.out.println(val.toString()+ "print");	
			String line = val.toString();
			String[] arr = line.split(" ");
	//		System.out.println(arr.length);
			String loc="";
			int critical = 0;
			int nonC = 0;
			int noV = 0;
			if(arr.length==5){
				System.out.println("exec");
				loc=arr[0]+" "+arr[1];
				 critical = Integer.parseInt(arr[2]);
				 nonC = Integer.parseInt(arr[3]);
				 noV = Integer.parseInt(arr[4]);
			}
			else{
			loc = arr[0];
			critical = Integer.parseInt(arr[1]);
			nonC = Integer.parseInt(arr[2]);
			noV = Integer.parseInt(arr[3]);
			}
			
			
			double total = (double)(critical + nonC + noV);
			double percent = ((double)(critical)/(double)(total))*100;
		//	System.out.println(loc + " " + percent + "percent");
			if(percent<min){
				min = percent;
				area = loc;
			}
		}
	//	System.out.println(area + "  " + Math.round(min));
		context.write(new Text(area), new Text(Math.round(min) + ""));
		
	}
}
public static void main(String[] args) throws Exception{
	Configuration conf= new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	Job job = new Job(conf, "Safest Area");
	
	job.setJarByClass(SafestArea.class);
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
