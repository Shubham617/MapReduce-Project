package shubham;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitionReducer extends Reducer<Text,Text,Text,Text> {
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
	
	for(Text val: values){
		String[] arr = val.toString().split("\t");
		if(arr.length==17 || arr[12].equals("Not Critical") || arr[12].equals("Not Applicable")){
			context.write( new Text(arr[2]),new Text(arr[1] + "  (" + arr[4]+")") );
		}
	}
}
}
