package Reducer;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Supports.StockObject;
import Supports.SupportObject;

public class Job1Reducer
	extends	Reducer<Text, StockObject, Text, StockObject > {
	
	@Override
	public void reduce(Text key, Iterable<StockObject> values,
			Context context) throws IOException, InterruptedException {
		
	}
 }
