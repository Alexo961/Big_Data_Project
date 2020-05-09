package Reducer;


import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.xbill.DNS.TXTRecord;

import Supports.OutputObject;
import Supports.StockObject;
import Supports.SupportObject;


public class Job1Reducer extends
Reducer<Text, Text, Text, Text > {


	private Map<Text, OutputObject> reduceMap = new HashMap<Text, OutputObject>();
	private Map<Text, Double> daSortareMap = new HashMap<Text, Double>();
	Double variation;

	StockObject so;

	Double valore_min;
	Double valore_max;
	StringBuilder sb;

	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		long sum = 0;
		int k= 0;
		Double[]coupleDouble = new Double[2];
		coupleDouble[0] = null;
		coupleDouble[0] = null;
		StockObject[] first_last = new StockObject[2];
		first_last[0] = null;
		first_last[1] = null;
		OutputObject output;


		for(Text value : values){
			List<String> list = Arrays.asList(value.toString().split(","));
			so = SupportObject.transform(list);
			System.out.println("CHIAVE:");
			System.out.println(key.toString());
			System.out.println(so.getTicker());
			System.out.println(so.getClose().toString());
			if (so != null) {
				first_last = SupportObject.first_last(first_last, so);
				sum += so.getVolume().longValue();
				k +=1;
				coupleDouble = SupportObject.min_max(coupleDouble, so.getClose());
			}
		}

		Double volume_medio = SupportObject.vol_med(sum, k);

		variation = SupportObject.variationquot(first_last[0].getOpen(), first_last[1].getClose());
		//inseriamo in mappa da sortare text e variation
		valore_min = coupleDouble[0];
		valore_max = coupleDouble[1];

		//inseriamo in mappa reduce map text e output object
		output = new OutputObject(key, variation, valore_min, valore_max, volume_medio);
		reduceMap.put(key, output);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		daSortareMap = SupportObject.sorted_dasortare(reduceMap);
		Map<Text, Double> sortedMap = SupportObject.sortByValues(daSortareMap);
		int numero_chiavi= daSortareMap.keySet().size();
		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == numero_chiavi) {
				break;
			}
			context.write(key,new Text( reduceMap.get(key).toString()));
		}



	}


}


