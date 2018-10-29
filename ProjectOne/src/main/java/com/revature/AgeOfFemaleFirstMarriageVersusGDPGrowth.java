package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.revature.util.Utility.*;

/*
 * MapReduce to see correlation between gdp growth and age of first marriage for females
 * 
 * Hypothesis: Countries with young marriages will have lower gdp growth
 * 
 * This MapReduce takes in LongWritable, Text and outputs Text, MapWritable
 * Mapper maps each country with the most recent age of first marriage
 * and the most recent gdp growth percentage
 * Reducer groups the output by age and averages the gdp growth
 */
public class AgeOfFemaleFirstMarriageVersusGDPGrowth {
	
	/*
	 * Mapper that takes in LongWritable, Text
	 * and outputs Text, MapWritable
	 * Output data determines mappings of country's age of 
	 * first marriage with gdp growth
	 */
	public static class AgeOfFemaleFirstMarriageMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			// parse the data by splitting on ","
			String[] columnSplit = parsedInput(value);
			
			if (columnSplit[SERIESCODE].equals(AGEFIRSTMARRIAGE)){
				MapWritable age = new MapWritable();
				for (int index = columnSplit.length-1; index > YEAR1960; index--){
					if (!columnSplit[index].equals(EMPTY)){
						age.put(new Text(AGEFIRSTMARRIAGE), new DoubleWritable(Double.parseDouble(columnSplit[index])));
						context.write(new Text(columnSplit[COUNTRYABB]), age);
						break;
					}
				}
			}
			if (columnSplit[SERIESCODE].equals(GDPGROWTH)){
				MapWritable gdp = new MapWritable();
				for (int index = columnSplit.length-1; index > YEAR1960; index--){
					if (!columnSplit[index].equals(EMPTY)){
						gdp.put(new Text(GDPGROWTH), new DoubleWritable(Double.parseDouble(columnSplit[index])));
						context.write(new Text(columnSplit[COUNTRYABB]), gdp);
						break;
					}
				}
			}		
		}	
		
	}
	
	/*
	 * Reducer takes in Text, MapWritable
	 * and outputs Text, DoubleWritable
	 * Output identifies first marriage age groups with avg
	 * gdp growth percentage for that group
	 */
	public static class AgeAndGDPGrowthCombiner extends Reducer<Text, MapWritable, Text, DoubleWritable>{
		
		public static DoubleWritable GROUP1 = new DoubleWritable();
		public static volatile double G1COUNT = 0.0;
		public static volatile DoubleWritable GROUP2 = new DoubleWritable();
		public static volatile double G2COUNT = 0.0;
		public static volatile DoubleWritable GROUP3 = new DoubleWritable();
		public static volatile double G3COUNT = 0.0;
		public static volatile DoubleWritable GROUP4 = new DoubleWritable();
		public static volatile double G4COUNT = 0.0;
		public static volatile DoubleWritable GROUP5 = new DoubleWritable();
		public static volatile double G5COUNT = 0.0;
		
		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{

			MapWritable age = new MapWritable();
			MapWritable gdp = new MapWritable();
			DoubleWritable ageVal = new DoubleWritable();
			DoubleWritable gdpVal = new DoubleWritable();
		
			for (MapWritable mw : values){
				for (Writable txt : mw.keySet()){
					if (((Text)txt).toString().equals(AGEFIRSTMARRIAGE)){
						age = mw;
						ageVal = (DoubleWritable) mw.get(txt);
					}
					else{
						gdp = mw;
						gdpVal = (DoubleWritable) mw.get(txt);
					}
				}
			}
			
			if (age != null && gdp != null && ageVal.get() != 0 && gdpVal.get() != 0){
				if(ageVal.get() < 20.0){
					GROUP1.set(GROUP1.get() + gdpVal.get());
					G1COUNT++;
				}
				else if(ageVal.get() >= 20.0  && ageVal.get() <= 23.0) {
					GROUP2.set(GROUP2.get() + gdpVal.get());
					G2COUNT++;
				}
				else if(ageVal.get() > 23.0  && ageVal.get() <= 27.0) {
					GROUP3.set(GROUP3.get() + gdpVal.get());
					G3COUNT++;
				}
				else if(ageVal.get() > 27.0  && ageVal.get() <= 30.0) {
					GROUP4.set(GROUP4.get() + gdpVal.get());
					G4COUNT++;
				}
				else{
					GROUP5.set(GROUP5.get() + gdpVal.get());
					G5COUNT++;
				}
			}				
		}
		
		/*
		 * runs once per reducer
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			if (G1COUNT > 0.0){
				context.write(new Text("Age < 20"), new DoubleWritable(DoubleFormat(GROUP1.get()/G1COUNT)));
			}
			if (G2COUNT > 0.0){
				context.write(new Text("20 <= Age <= 23"), new DoubleWritable(DoubleFormat(GROUP2.get()/G2COUNT)));
			}
			if (G3COUNT > 0.0){
				context.write(new Text("23 < Age <= 27"), new DoubleWritable(DoubleFormat(GROUP3.get()/G3COUNT)));
			}
			if (G4COUNT > 0.0){
				context.write(new Text("27 < Age <= 30"), new DoubleWritable(DoubleFormat(GROUP4.get()/G4COUNT)));
			}
			if (G5COUNT > 0.0){
				context.write(new Text("Age > 30"), new DoubleWritable(DoubleFormat(GROUP5.get()/G5COUNT)));
			}
		}
		
	}
	
	// tried to make partitioner but to no avail
	public static class AgeFirstMarriagePartitioner extends Partitioner<Text, MapWritable>{

		@Override
		public int getPartition(Text key, MapWritable combined, int numReduceTasks) {
			
			if(numReduceTasks == 0){
				return 0;
			}
			DoubleWritable age = new DoubleWritable();
			for (Writable dw : combined.keySet()){
				if (((Text)dw).toString().equals(AGEFIRSTMARRIAGE)){
					age = (DoubleWritable) combined.get(dw);
				}
				
			}
			
			if(age.get() < 20.0){
				return 0 % numReduceTasks;
			}
			else if(age.get() >= 20.0  && age.get() <= 23.0) {
				return 1 % numReduceTasks;
			}
			else if(age.get() >= 24.0  && age.get() <= 26.0) {
				return 2 % numReduceTasks;
			}
			else if(age.get() >= 27.0  && age.get() <= 30.0) {
				return 3 % numReduceTasks;
			}
			else{
				return 4 % numReduceTasks;
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Job job = new Job();
		job.setJarByClass(AgeOfFemaleFirstMarriageVersusGDPGrowth.class);
		job.setJobName("Age of Female First Marriage VS. GDP Growth");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setMapperClass(AgeOfFemaleFirstMarriageMapper.class);
		
		job.setReducerClass(AgeAndGDPGrowthCombiner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
