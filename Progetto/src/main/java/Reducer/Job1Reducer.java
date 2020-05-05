package Reducer;


import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Supports.OutputObject;
import Supports.StockObject;
import Supports.SupportObject;


public class Job1Reducer extends
Reducer<Text, StockObject, Text, Text > {


	private Map<Text, OutputObject> reduceMap = new HashMap<Text, OutputObject>();
	private Map<Text, Double> daSortareMap = new HashMap<Text, Double>();
	Double variation;
	
	
	Double valore_min;
	Double valore_max;
	StringBuilder sb;
	int numero_chiavi;

	public void reduce(Text key, Iterable<StockObject> values,
			Context context) throws IOException, InterruptedException {

		
		int sum = 0;
		int k= 0;
		Double[]a = new Double[2];
		StockObject[] first_last = new StockObject[2];


		for(StockObject value : values){
			first_last = SupportObject.first_last(first_last, value);
			sum += value.getVolume();
			k +=1;
			a = SupportObject.min_max(a, value.getClose());
		}

		Double volume_medio = SupportObject.vol_med(sum, k);

		variation = SupportObject.variationquot(first_last[0].getOpen(), first_last[1].getClose());
		//inseriamo in mappa da sortare text e variation
		valore_min = a[0];
		valore_max = a[1];

         //inseriamo in mappa reduce map text e output object
		numero_chiavi= daSortareMap.keySet().size();


	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<Text, Double> sortedMap = sortByValues(daSortareMap);
		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == numero_chiavi) {
				break;
			}
			context.write(key, reduceMap.get(key));
		}



	}

}
