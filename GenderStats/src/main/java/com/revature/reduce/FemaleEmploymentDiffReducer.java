package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleEmploymentDiffReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
private List<Double> rates = new ArrayList<>();
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for(DoubleWritable value : values) {
				rates.add(value.get());
		}
		//write the difference (change) between 2000 and the most recent year
		context.write(key, new DoubleWritable(rates.get(rates.size()-1) - rates.get(0)));
	}
}
