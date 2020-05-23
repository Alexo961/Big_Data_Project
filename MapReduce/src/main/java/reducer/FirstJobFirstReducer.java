package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import comparator.JobOneVariationComp;
import objects.StockObject;
import outputobjects.JobOneOutOne;
import support.JobSupports;
import support.ObjectSupports;

public class FirstJobFirstReducer
extends Reducer<Text, Text, Text, Text> {

	private Map<String, JobOneOutOne> reduceMap = new HashMap<String, JobOneOutOne>();
	//private Map<Text, Double> daSortareMap = new HashMap<Text, Double>();
	private List<JobOneOutOne> variationSet = new ArrayList<>();

	StockObject stock;
	double variation;
	double minimum;
	double maximum;
	double mediumVolume;


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		JobOneOutOne output;


		double[] minMaxPrice = null;
		StockObject[] firstLastStock = null;
		long sumOfVolumes = 0;
		int numValues = 0;
		//System.out.println(key.toString());
		for (Text value : values) {
			stock = (StockObject) ObjectSupports.textToActionObject(value);
			if (stock != null) {
				firstLastStock = JobSupports.firstLastStock(firstLastStock, stock);
				sumOfVolumes += stock.getVolume().longValue();
				numValues++;
				minMaxPrice = JobSupports
						.minMaxPrice(minMaxPrice, stock.getClose().doubleValue());
			}
		}

		mediumVolume = JobSupports.mediumVolume(sumOfVolumes, numValues);

		//System.out.println("MINPRICE: " + minMaxPrice[0]);
		//System.out.println("MANPRICE: " + minMaxPrice[1]);

		//if (firstLastStock[0] == null)
		//System.out.println("OPEN NULL " + key.toString());
		//if (firstLastStock[1] == null)
		//System.out.println("CLOSE NULL " + key.toString());
		variation = JobSupports
				.variationQuotation(firstLastStock[0].getOpen(), firstLastStock[1].getClose());
		minimum = minMaxPrice[0];
		maximum = minMaxPrice[1];
		//context.write(key, JobOneSupports.firstOutput(variation, minimum, maximum, mediumVolume));


		output = new JobOneOutOne(key.toString(), variation, minimum, maximum, mediumVolume);
		//System.out.println("PRIMA DEL PUT");
		//System.out.println(key.toString());
		reduceMap.put(key.toString(), output);
		//System.out.println("DOPO DEL PUT");
		//System.out.println(key.toString());
		variationSet.add(output);



	}

	/*
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		daSortareMap = JobOneSupports.sorted_dasortare(reduceMap);
		Map<Text, Double> sortedMap = JobOneSupports.sortByValues(daSortareMap);
		int numero_chiavi= daSortareMap.keySet().size();
		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == numero_chiavi) {
				//System.out.println("NUMERO CHIAVI:");
				//System.out.println(numero_chiavi);
				break;
			}
			//System.out.println("NUMERO CHIAVI:");
			//System.out.println(numero_chiavi);
			//System.out.println(reduceMap.size());
			context.write(key, JobOneSupports.outOneToText(reduceMap.get(key)));
		}
	}
	 */
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException{
		
		variationSet.sort(new JobOneVariationComp());
		Collections.reverse(variationSet);

		Iterator<JobOneOutOne> itr = variationSet.iterator();
		while (itr.hasNext()){
			JobOneOutOne out = itr.next();
			context.write(new Text(out.getTicker()), JobSupports.outOneToText(out));
		}
	}
}
