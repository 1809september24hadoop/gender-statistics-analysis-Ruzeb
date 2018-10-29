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
 * MapReduce to identify countries where % of female graduates is less than 30.
 * 
 * Assumptions: 
 * => Graduates are those who have graduated from first degree programmes (ISCED 6 & 7),
 * => takes in three most recent percentages (if possible)
 * 
 * This MapReduce takes in LongWritable, Text and outputs Text, DoubleWritable
 * Mapper shows each country's female graduates, 
 * Reducer shows countries that have less than 30% female graduates
 */
public class CountriesFGraduatesLtThirty {
	
	/*
	 * Mapper that takes in LongWritable, Text
	 * and outputs Text, DoubleWritable
	 * Output data determines each country's female graduates based on assumptions
	 */
	public static class CountryFGraduatesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		// constant for series codes pertaining to female graduates
		public static final String TERTIARYGRAD = "SE.TER.CMPL.FE.ZS";
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			// parse the data by splitting on ","
			String[] columnSplit = parsedInput(value);
			
			//map three most recent percentages (if possible)
			if (columnSplit[SERIESCODE].equals(TERTIARYGRAD)){
				for (int index = columnSplit.length - 1, counter = 3; counter > 0 && index > YEAR1960; index--){
					if (!columnSplit[index].equals(EMPTY)){
						context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(columnSplit[index])));
						counter--;
					}
				}
			}			
		}	
		
	}
	
	/*
	 * Reducer takes in Text, DoubleWritable
	 * and outputs Text, DoubleWritable
	 * Output identifies countries containing % female graduates less than 30 based on the assumptions
	 */
	public static class AverageCountryFGraduatesLtThirtyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

			double totalPercentage = 0;
			
			int counter = 0;
			for (DoubleWritable value: values){
				totalPercentage += value.get();
				counter++;
			}
			
			totalPercentage /= counter;
			
			if (totalPercentage < 30.0){
				context.write(key, new DoubleWritable(DoubleFormat(totalPercentage)));
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Job job = new Job();
		job.setJarByClass(CountriesFGraduatesLtThirty.class);
		job.setJobName("Countries with female graduates < 30%");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(CountryFGraduatesMapper.class);
		job.setReducerClass(AverageCountryFGraduatesLtThirtyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
