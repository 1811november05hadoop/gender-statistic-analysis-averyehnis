package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USFemaleEducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String newLine = line.substring(1, line.length()-1);
		String[] splitLine = newLine.split("\",\"");

		if(line.contains("United States") && line.contains("female") &&
				line.contains("Educational attainment, completed") && 
				!line.contains("primary") && !line.contains("lower secondary")) {
			//44 is the index of year 2000
			for(int i = 44; i < splitLine.length; i++) {
				//Will only consider the years with data between 2000 and 2016
				if (splitLine[i].matches("[0-9]+.[0-9]+")) {
					//Write the type of education and each year's data
					context.write(new Text(splitLine[2]), new DoubleWritable(Double.valueOf(splitLine[i])));
				}
			}
		}
	}
}
