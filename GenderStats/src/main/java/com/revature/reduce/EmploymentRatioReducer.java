package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmploymentRatioReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for(DoubleWritable value : values) {
			//Will only print countries where the ratio is more than 6.6:1 or less than 1:6.6
			if(value.get() > 15.0 || value.get() < -15.0) {
				context.write(key, value);
			}
		}
	}
}
