package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmploymentRatioMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String newLine = line.substring(1, line.length()-1);
		String[] splitLine = newLine.split("\",\"");

		if(line.contains("labor force participation rate") && line.contains("Ratio")) {
			for(int i = splitLine.length-1; i >= 4; i--) {
				//Will check if there is data for the country
				if (splitLine[i].matches("[0-9]+.[0-9]+")) {
					//If there's data, write the most recent data only
					context.write(new Text(splitLine[0] + " | " + splitLine[2]), new DoubleWritable(Double.valueOf(splitLine[i])));
					break;
				}
			}
		}
	}
}
