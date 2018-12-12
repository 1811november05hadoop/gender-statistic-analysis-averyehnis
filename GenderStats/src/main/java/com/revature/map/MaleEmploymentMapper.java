package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String newLine = line.substring(1, line.length()-1);
		String[] splitLine = newLine.split("\",\"");

		if(line.contains(" male") && line.contains("Employment to population") && 
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
