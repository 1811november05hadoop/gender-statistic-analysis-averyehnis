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

import com.revature.map.USFemaleEducationMapper;
import com.revature.reduce.USFemaleEducationAvgReducer;

public class USFemaleEducationTest {
	String testInput = "\"United States\",\"USA\",\"Educational attainment, completed upper"
			+ " secondary, population 25+, female (%)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"60.854\",\"\",\"62.854\",\"\",\"64.854\""
			+ ",\"\",\"66.854\",\"\",\"68.854\",\"\",\"70.854\",\"72.854\",\"\",\"74.854\","
			+ "\"77.854\",\"78.854\",\"80.854\"";
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text,DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	private String textOutput = "Educational attainment, completed upper secondary, population 25+, female (%)";
	
	@Before
	public void setup() {
		USFemaleEducationMapper mapper = new USFemaleEducationMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		USFemaleEducationAvgReducer reducer = new USFemaleEducationAvgReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(60.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(62.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(64.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(66.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(68.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(70.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(72.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(74.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(77.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(78.854));
		mapDriver.withOutput(new Text(textOutput), new DoubleWritable(80.854));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(60.854));
		values.add(new DoubleWritable(62.854));
		values.add(new DoubleWritable(64.854));
		values.add(new DoubleWritable(66.854));
		values.add(new DoubleWritable(68.854));
		values.add(new DoubleWritable(70.854));
		values.add(new DoubleWritable(72.854));
		values.add(new DoubleWritable(74.854));
		values.add(new DoubleWritable(77.854));
		values.add(new DoubleWritable(78.854));
		values.add(new DoubleWritable(80.854));
		
		
		reduceDriver.withInput(new Text(textOutput), values);
		reduceDriver.withOutput(new Text(textOutput), new DoubleWritable(2.0));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapReduceDriver.withOutput(new Text(textOutput), new DoubleWritable(2.0));
		
		mapReduceDriver.runTest();
	}
}
