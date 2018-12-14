package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.EmploymentRatioMapper;
import com.revature.reduce.EmploymentRatioReducer;

public class EmploymentRatioTest {
	String testInput = "\"Arab World\",\"ARB\",\"Ratio of female to male labor force participation"
			+ " rate (%) (modeled ILO estimate)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"34.12309487\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"";
	
	String textOutput = "Arab World | Ratio of female to male labor force participation rate (%) (modeled ILO estimate)";
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text,DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup() {
		EmploymentRatioMapper mapper = new EmploymentRatioMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		EmploymentRatioReducer reducer = new EmploymentRatioReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(34.12309487));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(34.12309487));
		
		reduceDriver.withInput(new Text(textOutput), values);
		reduceDriver.withOutput(new Text(textOutput), new DoubleWritable(34.12309487));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapReduceDriver.withOutput(new Text(textOutput), new DoubleWritable(34.12309487));
		
		mapReduceDriver.runTest();
	}
}
