package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static com.revature.util.Utility.*;

/*
 * MapReduce to determine percent change in world female employment from 2000 for 15+ males
 * 
 * Assumptions: 
 * => World data is correct and indicative of all countries' female employment within five percent margin of error.  
 * => Employment is defined as persons of working age who, during a short reference period, were engaged in any 
 *    activity to produce goods or provide services for pay or profit, whether at work during the reference period 
 *    (i.e. who worked in a job for at least one hour) or not at work due to temporary absence from a job, or to 
 *    working-time arrangements. Ages 15 and older are generally considered the working-age population.
 * 
 * This MapReduce takes in LongWritable, Text and outputs Text, DoubleWritable
 * Mapper lists World female employment from 2000 to 2016 (if possible)
 * Reducer calculates the percent change in female employment
 */
public class PercentChangeInFEmployment {

	/*
	 * Mapper that takes in LongWritable, Text
	 * and outputs Text, DoubleWritable
	 * Output data determines World's female employment from year 2000
	 */
	public static class FemaleEmployment2000Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		// constant pertaining to male employment 
		public static final String WorldFemaleEmploymentFrom2000 = "SL.EMP.TOTL.SP.FE.ZS";

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] columnSplit = parsedInput(value);
			
			if (columnSplit[COUNTRYABB].equals(WORLD)){
				if (columnSplit[SERIESCODE].equals(WorldFemaleEmploymentFrom2000)){
					for (int index = YEAR2000; index < columnSplit.length; index++){
						if (!columnSplit[index].equals(EMPTY)){
							context.write(new Text(columnSplit[COUNTRYABB]), new DoubleWritable(Double.parseDouble(columnSplit[index])));
						}
					}
				}
			}
		}
		
	}

	/*
	 * Reducer takes in Text, DoubleWritable
	 * and outputs Text, DoubleWritable
	 * Output is the percent change in World employment for males ages 15+ from the year 2000
	 */
	public static class PercentChangeFemaleEmploymentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			
			double Y2K = 0.0;
			double Y2K16 = 0.0;
			
			if (values.iterator().hasNext()){
				Y2K16 = values.iterator().next().get();
			}
			while(values.iterator().hasNext()){
				Y2K = values.iterator().next().get();
			}
			
			context.write(new Text("Percent Change in Female Employment From 2000"), new DoubleWritable(100.0*DoubleFormat((Y2K16-Y2K)/Y2K)));	
		}
		
	}

	public static void main(String[] args) throws Exception{
		
		Job job = new Job();
		job.setJarByClass(PercentChangeInFEmployment.class);
		job.setJobName("Percent Change in Female Employment From 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setMapperClass(FemaleEmployment2000Mapper.class);
		job.setReducerClass(PercentChangeFemaleEmploymentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}

