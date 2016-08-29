package shubham;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import shubham.SafestArea.MyMapper;
import shubham.SafestArea.MyReducer;

import java.util.*;


public class SafestAreaTesting {

	MapReduceDriver<LongWritable,Text,Text,Text,Text,Text> mapReduceDriver;
	MapDriver<LongWritable,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;
	
	@Before
	public void setUp() {
		MyMapper mapper = new MyMapper();
		MyReducer reducer = new MyReducer();
		
		mapDriver = new MapDriver<LongWritable,Text,Text,Text>();
		mapDriver.setMapper(mapper);
		
		reduceDriver = new ReduceDriver<Text,Text,Text,Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
		
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(0), new Text("BRONX	Critical Violation: 21481  Non Critical Violations: 17552  No Violations: 806"));
		mapDriver.withOutput(new Text("Area"),new Text("BRONX 21481 17552 806"));
		mapDriver.runTest();
	}

}
