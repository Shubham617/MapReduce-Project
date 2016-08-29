package shubham;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

public class AreaPartitioner extends Partitioner<Text,Text> {
@Override
public int getPartition(Text key, Text value, int numReduceTasks){
	String[] arr=value.toString().split("\t");
	String area = arr[2];
	
	if(numReduceTasks == 0){
		return 0;
	}
	
	if(area.equals("MANHATTAN")){
		return 0;
	}
	if(area.equals("BRONX")){
		return 1;
	}
	if(area.equals("BROOKLYN")){
		return 2;
	}
	if(area.equals("QUEENS")){
		return 3;
	}
	return 4;
	
}
}
