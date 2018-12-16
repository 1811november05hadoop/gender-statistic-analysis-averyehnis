package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper pulls the ratio of females over age 15 to the total population
 *   and the data from the year 2000 and onward
 * The reducer will use the most recent data and compare it to the data from 2000,
 *   and determine the percentage increase.
 *
 */
public class FemaleEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String newLine = line.substring(1, line.length()-1);
		String[] splitLine = newLine.split("\",\"");

		if(line.contains("female") && line.contains("Employment to population") && 
				line.contains("15+")) {
			//44 is the index of year 2000
			for(int i = 44; i < splitLine.length; i++) {
				//Will only consider the years between 2000 and 2016 that have data
				if (splitLine[i].matches("[0-9]+.[0-9]+")) {
					//Write the country and each year's data
					context.write(new Text(splitLine[0]), new DoubleWritable(Double.valueOf(splitLine[i])));
				}
			}
		}
	}
}
