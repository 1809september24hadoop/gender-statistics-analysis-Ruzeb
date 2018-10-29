package com.revature.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.revature.AgeOfFemaleFirstMarriageVersusGDPGrowth.AgeAndGDPGrowthCombiner;
import com.revature.AgeOfFemaleFirstMarriageVersusGDPGrowth.AgeOfFemaleFirstMarriageMapper;
import static com.revature.util.Utility.*;


public class AgeOfFemaleFirstMarriage {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private static MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
	private static ReduceDriver<Text, MapWritable, Text, DoubleWritable> reduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, DoubleWritable> mapReduceDriver;
	
	private static MapWritable mw = new MapWritable();
	private static MapWritable mw2 = new MapWritable();
	
	private static String input1 = "\"United States\",\"USA\",\"Age at first marriage, female\",\"SP.DYN.SMAM.FE\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"21.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"23.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"25.4\",\"\",\"\",\"\",\"\",\"23.3\",\"\","
			+ "\"\",\"\",\"\",\"26\",\"\",\"23.7\",\"\",\"\",\"\",\"\",\"23.7\",\"\",\"26.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
	private static String input2 = "\"United States\",\"USA\",\"GDP growth (annual %)\",\"NY.GDP.MKTP.KD.ZG\",\"\","
			+ "\"2.29999999999977\",\"6.09999999999999\",\"4.40000000000002\",\"5.80000000000014\",\"6.39999999999984\","
			+ "\"6.50000000000024\",\"2.50000000000003\",\"4.79999999999974\",\"3.10000000000004\",\"3.20680725745444\","
			+ "\"3.29547672836176\",\"5.26326277619982\",\"5.64312484755203\",\"-0.51715456222523\",\"-0.197678536519447\","
			+ "\"5.38609005075547\",\"4.60859740653179\",\"5.5616849289446\",\"3.17569075012061\",\"-0.244596225208085\","
			+ "\"2.59447038823151\",\"-1.91089106804856\",\"4.63245718120484\",\"7.25908695936059\",\"4.23873752083914\","
			+ "\"3.5116144990922\",\"3.46174769185008\",\"4.20397197941296\",\"3.68052403304715\",\"1.91937029742549\","
			+ "\"-0.0740845307123976\",\"3.55539614766758\",\"2.74585671892275\",\"4.03764342486481\",\"2.71897578878193\","
			+ "\"3.79588122942587\",\"4.48702649316731\",\"4.44991096328404\",\"4.68519960839866\",\"4.09217644881066\","
			+ "\"0.975981833932124\",\"1.78612768745552\",\"2.80677595648093\",\"3.78574284969444\",\"3.34521606334877\","
			+ "\"2.666625826122\",\"1.77857023965289\",\"-0.291621458693953\",\"-2.77552957416808\",\"2.53192061616315\","
			+ "\"1.60145467247139\",\"2.22403085385714\",\"1.67733152992453\",\"2.37045767146387\",\"2.59614804050973\",\"\",";
	

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@BeforeClass
	public static void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		AgeOfFemaleFirstMarriageMapper mapper = new AgeOfFemaleFirstMarriageMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, MapWritable>();
		mapDriver.setMapper(mapper);
		
		mw.put(new Text(AGEFIRSTMARRIAGE), new DoubleWritable(Double.parseDouble("26.9")));
		mw2.put(new Text(GDPGROWTH), new DoubleWritable(Double.parseDouble("2.59614804050973")));

		/*
		 * Set up the reducer test harness.
		 */
		AgeAndGDPGrowthCombiner reducer = new AgeAndGDPGrowthCombiner();
		reduceDriver = new ReduceDriver<Text, MapWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text(input1));

		try {
			for(Pair<Text, MapWritable> output: mapDriver.run()) {
				for(Writable key: output.getSecond().keySet()) {
					for (Writable ew : mw.keySet()){
						assertEquals(mw.get(ew), output.getSecond().get(key));
					}
					
				}
			}
		} 
		catch (IOException e) {
		}

	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		List<MapWritable> values = new ArrayList<>();
		values.add(mw);
		values.add(mw2);

		reduceDriver.withInput(new Text("USA"), values);

		reduceDriver.withOutput(new Text("23 < Age <= 27"), new DoubleWritable(2.59614804050973));

		/*
		 * Run the test.
		 */
		reduceDriver.runTest();
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() {

		mapReduceDriver.addInput(new LongWritable(1), new Text(input1));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}