package com.revature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static com.revature.util.Utility.*;

/*
 * MapReduce to identify the average increase in US female education from 2000
 * 
 * Assumptions: 
 * => Female education is synonymous with Primary and Secondary education
 * 
 * This MapReduce takes in LongWritable, Text and outputs Text, DoubleWritable
 * Mapper outputs an ArrayWritable containing female education percentages from 2000 onwards
 * for both Primary and Secondary education
 * Reducer shows the average increase in both Primary and Secondary female education in the US from 2000
 */
public class IncreaseInUSFemaleEducationFrom2000 {
	
	/*
	 * DoubleArrayWritable is a subclass of ArrayWritable
	 */
	public static class DoubleArrayWritable extends ArrayWritable{
		public DoubleArrayWritable() {
			super(DoubleWritable.class);
		}
	}

	/*
	 * Mapper that takes in LongWritable, Text
	 * and outputs Text, DoubleArrayWritable
	 * Outputs ArrayWritable of DoubleWritables for each year from 2000 onwards
	 * for US Female education in Primary and Secondary education
	 */
	public static class USFemaleEducationFrom2000Mapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>{

		public static final String PRIMARYENROLLMENTFEMALES = "SE.PRM.NENR.FE";
		public static final String SECONDARYENROLLMENTFEMALES = "SE.SEC.NENR.FE";

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] columnSplit = parsedInput(value);
			
			if (columnSplit[COUNTRYABB].equals(USA)){
				if (columnSplit[SERIESCODE].equals(PRIMARYENROLLMENTFEMALES) || columnSplit[SERIESCODE].equals(SECONDARYENROLLMENTFEMALES)){
					List<DoubleWritable> arrList = new ArrayList<>();
					
					for (int index = YEAR2000; index < columnSplit.length; index++){
						if (!columnSplit[index].equals(EMPTY)){
							arrList.add(new DoubleWritable(Double.parseDouble(columnSplit[index])));
						}
					}
					DoubleArrayWritable arr = new DoubleArrayWritable();
					arr.set(arrList.toArray(new DoubleWritable[arrList.size()]));
					context.write(new Text(columnSplit[DESC]), arr);
				}
			}
		}	
	}

	/*
	 * Reducer takes in Text, DoubleArrayWritable
	 * and outputs Text, DoubleWritable
	 * Output identifies the average increase in US female education for 
	 * Primary and Secondary education from 2000 onwards
	 */
	public static class IncreaseInEducationReducer extends Reducer<Text, DoubleArrayWritable, Text, DoubleWritable>{
		
		@Override
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException{
			
			double differences = 0.0;;
			double average = 0.0;
			
			DoubleArrayWritable dubArr = new DoubleArrayWritable();
			if (values.iterator().hasNext()){
				dubArr = values.iterator().next();	
			}
			
			Writable[] arr = dubArr.get();
			
			for (int index = 0; index + 1 < arr.length; index++){
				differences += ((DoubleWritable)arr[index+1]).get() - ((DoubleWritable) arr[index]).get();
			}
			
			average = differences / (arr.length - 1.0);
			context.write(key, new DoubleWritable(DoubleFormat(average)));
			
		}
	}

	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(IncreaseInUSFemaleEducationFrom2000.class);
		job.setJobName("Average Increase in USA Female Education from Year 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);

		job.setMapperClass(USFemaleEducationFrom2000Mapper.class);
		job.setReducerClass(IncreaseInEducationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}


}
