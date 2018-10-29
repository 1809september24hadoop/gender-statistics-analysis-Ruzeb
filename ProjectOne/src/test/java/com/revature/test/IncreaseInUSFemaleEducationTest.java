package com.revature.test;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import com.revature.IncreaseInUSFemaleEducationFrom2000.IncreaseInEducationReducer;
import com.revature.IncreaseInUSFemaleEducationFrom2000.USFemaleEducationFrom2000Mapper;
import com.revature.IncreaseInUSFemaleEducationFrom2000.DoubleArrayWritable;;


public class IncreaseInUSFemaleEducationTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private static MapDriver<LongWritable, Text, Text, DoubleArrayWritable> mapDriver;
	private static ReduceDriver<Text, DoubleArrayWritable, Text, DoubleWritable> reduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, Text, DoubleWritable> mapReduceDriver;
	
	private static List<DoubleWritable> arrList = new ArrayList<DoubleWritable>();
	
	private static DoubleArrayWritable arr = new DoubleArrayWritable();
	
	private static String input1 = "\"United States\",\"USA\",\"School enrollment, primary, female (% net)\","
			+ "\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\","
			+ "\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\","
			+ "\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\","
			+ "\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@BeforeClass
	public static void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		USFemaleEducationFrom2000Mapper mapper = new USFemaleEducationFrom2000Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleArrayWritable>();
		mapDriver.setMapper(mapper);
		
		arrList.add(new DoubleWritable(96.84398));
		arrList.add(new DoubleWritable(97.07956));
		arrList.add(new DoubleWritable(95.69427));
		arrList.add(new DoubleWritable(95.43967));
		arrList.add(new DoubleWritable(94.11045));
		arrList.add(new DoubleWritable(94.42654));
		arrList.add(new DoubleWritable(95.79755));
		arrList.add(new DoubleWritable(96.439));
		arrList.add(new DoubleWritable(96.82568));
		arrList.add(new DoubleWritable(95.78414));
		arrList.add(new DoubleWritable(93.85194));
		arrList.add(new DoubleWritable(93.59352));
		arrList.add(new DoubleWritable(92.98951));
		arrList.add(new DoubleWritable(92.62706));
		arrList.add(new DoubleWritable(93.32211));
		arrList.add(new DoubleWritable(94.0818));
		
		arr.set(arrList.toArray(new DoubleWritable[arrList.size()]));

		/*
		 * Set up the reducer test harness.
		 */
		IncreaseInEducationReducer reducer = new IncreaseInEducationReducer();
		reduceDriver = new ReduceDriver<Text, DoubleArrayWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, Text, DoubleWritable>();
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
			for(Pair<Text, DoubleArrayWritable> output: mapDriver.run()) {
				for (int index = 0; index < arr.get().length; index++){
					assertEquals(arr.get()[index], output.getSecond().get()[index]);
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

		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(arr);

		reduceDriver.withInput(new Text("School enrollment, primary, female (% net)"), values);

		reduceDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(-0.18));

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

		mapReduceDriver.addOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(-0.18));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}