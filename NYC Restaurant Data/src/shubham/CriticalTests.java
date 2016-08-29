package shubham;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import shubham.CriticalViolations.MyMapper;
import shubham.CriticalViolations.MyReducer;


import java.util.*;

public class CriticalTests {

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
		mapDriver.withInput(new LongWritable(0), new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		//mapDriver.withInput(new LongWritable(0),new Text("41585567	PAPPARDELLA	MANHATTAN	316	COLUMBUS AVENUE	10023	2125957996	Italian	7/23/2014	Violations were cited in the following area(s).	04H	Raw, cooked or prepared food is adulterated, contaminated, cross-contaminated, or not discarded in accordance with HACCP plan.	Critical	7	A	7/23/2014	8/21/2016	Cycle Inspection / Re-inspection"));
		mapDriver.withOutput(new Text("THE MET GRILL/DOUBLE TREE HOTEL (569,LEXINGTON AVENUE,MANHATTAN)"),new Text("Food contact surface not properly maintained."));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("Food contact surface not properly maintained."));
		
		reduceDriver.withInput(new Text("THE MET GRILL/DOUBLE TREE HOTEL (569,LEXINGTON AVENUE,MANHATTAN)"), values);
		reduceDriver.withOutput(new Text("THE MET GRILL/DOUBLE TREE HOTEL (569,LEXINGTON AVENUE,MANHATTAN)"), new Text("Food contact surface not properly maintained."));
		
	}
	
	@Test
	public void testMapReduceSingleProduct(){
		mapReduceDriver.withInput(new LongWritable(0), new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		mapReduceDriver.addOutput(new Text("THE MET GRILL/DOUBLE TREE HOTEL (569,LEXINGTON AVENUE,MANHATTAN)"), new Text("Food contact surface not properly maintained."));
		mapReduceDriver.runTest();
	}
}
