package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.GradRateMapper;

public class GradRateTest {
	String testInput = "\"Arab World\",\"ARB\",\"Educational attainment, at least completed upper"
			+ " secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"25.38549210114536\",\"\",\"\",\"\",\"\",\"\",\"\"";
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	@Before
	public void setup() {
		GradRateMapper mapper = new GradRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(testInput));
		
		mapDriver.withOutput(new Text("Arab World"), new DoubleWritable(25.38549210114536));
		
		mapDriver.runTest();
	}
}
