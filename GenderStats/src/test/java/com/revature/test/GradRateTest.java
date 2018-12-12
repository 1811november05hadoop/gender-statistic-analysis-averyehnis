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

import com.revature.map.GradRateMapper;
import com.revature.reduce.GradRateReducer;

public class GradRateTest {
	String testInput = "\"Arab World\",\"ARB\",\"Educational attainment, at least completed upper"
			+ " secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"25.38549210114536\",\"\",\"\",\"\",\"\",\"\",\"\"";
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text,DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup() {
		GradRateMapper mapper = new GradRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		GradRateReducer reducer = new GradRateReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapDriver.withOutput(new Text("Arab World"), new DoubleWritable(25.38549210114536));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(25.38549210114536));
		
		reduceDriver.withInput(new Text("Arab World"), values);
		reduceDriver.withOutput(new Text("Arab World"), new DoubleWritable(25.38549210114536));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapReduceDriver.withOutput(new Text("Arab World"), new DoubleWritable(25.38549210114536));
		
		mapReduceDriver.runTest();
	}
}
