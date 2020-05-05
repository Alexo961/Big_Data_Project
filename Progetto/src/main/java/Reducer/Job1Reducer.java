package Reducer;


import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Supports.StockObject;
import Supports.SupportObject;


public class Job1Reducer extends
Reducer<Text, StockObject, Text, Text > {

	public void reduce(Text key, Iterable<StockObject> values,
		Context context) throws IOException, InterruptedException {

		Double variation;
		int sum = 0;
		int k= 0;
		Double[]a = new Double[2];
		StockObject[] first_last = new StockObject[2];
		Double min;
		Double max;
		StringBuilder sb;

		for(StockObject value : values){
			first_last = SupportObject.first_last(first_last, value);
			sum += value.getVolume();
			k +=1;
			a = SupportObject.min_max(a, value.getClose());
		}
		
		Double volume_medio = SupportObject.vol_med(sum, k);
		variation = SupportObject.variationquot(first_last[0].getOpen(), first_last[1].getClose());
		min = a[0];
		max = a[1];

	}

}
