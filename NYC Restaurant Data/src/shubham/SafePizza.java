package shubham;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.GenericMRLoadGenerator.SampleMapper;
import org.apache.hadoop.mapred.GenericMRLoadGenerator.SampleReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SafePizza {

	public static void main(String[] args){
		try{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			Job job = new Job(conf, "Pizza Places");
			job.setJarByClass(SafePizza.class);
			job.setMapperClass(PartitionMapper.class);
			job.setReducerClass(PartitionReducer.class);
			job.setPartitionerClass(AreaPartitioner.class);
			job.setNumReduceTasks(5);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			
			System.exit(job.waitForCompletion(true)?0:1);
		}
		catch(Exception e){e.printStackTrace();}
		
			
			
			
		}
	}

