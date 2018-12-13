package com.revature.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.USFemaleEducationMapper;
import com.revature.reduce.USFemaleEducationAvgReducer;

public class USFemaleEducation {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: GenderStats <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(USFemaleEducation.class);
		
		job.setJobName("USFemaleEducation");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(USFemaleEducationMapper.class);
		job.setReducerClass(USFemaleEducationAvgReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
