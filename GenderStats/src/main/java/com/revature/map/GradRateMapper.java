package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper pulls the most recent data for completion of upper 
 *   secondary education for each country
 * The reducer will determine which countries have a graduation rate
 *   of less than 30%
 *
 */
public class GradRateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String newLine = line.substring(1, line.length()-1);
		String[] splitLine = newLine.split("\",\"");
		//A default that will fail the reduce, so no data means not included
		double mostRecent = 100.00;

		if(line.contains("at least completed upper secondary") && line.contains("female")) {
			for(int i = splitLine.length-1; i >= 4; i--) {
				if (splitLine[i].matches("[0-9]+.[0-9]+")) {
					mostRecent = Double.valueOf(splitLine[i]);
					break;
				}
			}
		}
		
		context.write(new Text(splitLine[0]), new DoubleWritable(mostRecent));
	}
}
