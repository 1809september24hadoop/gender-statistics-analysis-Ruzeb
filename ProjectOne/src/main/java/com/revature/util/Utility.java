package com.revature.util;

import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;

public class Utility {
	
	public static final String EMPTY = "";
	public static final String USA = "USA";
	public static final String WORLD = "WLD";
	public static final String AGEFIRSTMARRIAGE = "SP.DYN.SMAM.FE";
	public static final String GDPGROWTH = "NY.GDP.MKTP.KD.ZG";
	
	public static final int YEAR1960 = 4;
	public static final int YEAR2000 = 44;
	public static final int SERIESCODE = 3;
	public static final int DESC = 2;
	public static final int COUNTRYABB = 1;
	public static final int COUNTRY = 0;
	
	public static String[] parsedInput(Text value){	
		String record = value.toString();
		String recordTrim = record.substring(1, record.length()-2);
		String[] columnSplit = recordTrim.split("\",\"");
		return columnSplit;
	}
	
	public static double DoubleFormat(double value){
		DecimalFormat twoDecimals = new DecimalFormat("#.##");
		return Double.parseDouble(twoDecimals.format(value));
	}

}
