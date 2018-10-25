package com.revature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountriesFGraduatesLtThirty {
	
	public static class CountryFGraduatesMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		
		public static final String BACHELORS25 = "SE.TER.HIAT.BA.FE.ZS";
		public static final String MASTERS25 = "SE.TER.HIAT.MS.FE.ZS";
		public static final String DOCTORATES25 = "SE.TER.HIAT.DO.FE.ZS";
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String record = value.toString();
			String recordTrim = record.substring(1, record.length()-2);
			String[] columnSplit = recordTrim.split("\",\"");
			//String[] columnSplit = record.split(",");
			
			for (int index = 0; index < columnSplit.length; index++){
				System.out.println(columnSplit[index] + " is at index " + index);
			}
			
			if (columnSplit[3].equals(BACHELORS25) || columnSplit[3].equals(MASTERS25) || columnSplit[3].equals(DOCTORATES25)){
				for (int index = 4; index < columnSplit.length; index++){
					String data = columnSplit[index];
					if (!data.equals("")){
						context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(data)));
					}
				}
			}
		}	
	}
	
	public static class AverageCountryFGraduatesLtThirtyReducer extends Reducer<Text, DoubleWritable, Text, Boolean>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			System.out.println("HELLO REDUCER");
			double totalPercentage = 0;
			
			int counter = 0;
			for (DoubleWritable value: values){
				totalPercentage += value.get();
				counter++;
			}
			
			totalPercentage /= counter;
			if (totalPercentage < 30.0){
				context.write(key, true);
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(CountriesFGraduatesLtThirty.class);
		job.setJobName("Countries With female graduates < 30%");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(CountryFGraduatesMapper.class);
		job.setReducerClass(AverageCountryFGraduatesLtThirtyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Boolean.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
