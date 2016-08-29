package shubham;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PartitionMapper extends Mapper<LongWritable,Text,Text,Text> {
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String line = value.toString();
	String[] arr=line.split("\t");
	
	if(arr.length!=17 && arr.length!=18){
		System.out.println("EXEC" + arr.length + "  " + line);
	}
	if(!arr[2].equals("BORO") && !arr[2].equals("Missing")){
		if(arr[7].equals("Pizza") || arr[7].equals("Pizza/Italian")){
	context.write(new Text(arr[7]),new Text(line));
		}
	}
}
}
