package Reducer;


import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Supports.StockObject;
import Supports.SupportObject;


public class Job1Reducer extends
Reducer<Text, StockObject, Text, Text > {

	public void reduce(Text key, Iterable<StockObject> values,
			Context context) throws IOException, InterruptedException {


		
		int sum = 0;
		int k= 0;
		Double[]a = new Double[2];
		
		for(StockObject value : values) {
			sum += value.getVolume();
			k +=1;
			 a = SupportObject.min_max(a, value.getClose());
		}
		Double volume_medio = SupportObject.vol_med(sum, k);



		




}

 }
