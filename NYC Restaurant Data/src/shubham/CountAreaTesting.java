package shubham;

import org.apache.hadoop.io.*;



import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import shubham.CountArea.MyMapper;
import shubham.CountArea.MyReducer;


import java.util.*;

public class CountAreaTesting {

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
		mapDriver.withOutput(new Text("MANHATTAN"),new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		values.add(new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Not Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		values.add(new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Not Applicable	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		values.add(new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		reduceDriver.withInput(new Text("MANHATTAN"), values);
		reduceDriver.withOutput(new Text("MANHATTAN"), new Text("Critical Violation: 1 Non Critical Violations: 1  No Violations: 2"));
	}
	
	@Test 
	public void testMapReduceSingleProduct(){
		mapReduceDriver.withInput(new LongWritable(0), new Text("41571350	THE MET GRILL/DOUBLE TREE HOTEL	MANHATTAN	569	LEXINGTON AVENUE	10022	2123506081	American	4/8/2016	Violations were cited in the following area(s).	09C	Food contact surface not properly maintained.	Critical	33			8/21/2016	Cycle Inspection / Initial Inspection"));
		mapReduceDriver.addOutput(new Text("MANHATTAN"), new Text("Critical Violation: 1  Non Critical Violations: 0  No Violations: 0"));
		mapReduceDriver.runTest();
	}
	
}
