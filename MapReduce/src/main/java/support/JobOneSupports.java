package support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;


import objects.StockObject;
import outputobjects.JobOneOutOne;

public class JobOneSupports {

	public static StockObject[] firstLastStock(StockObject[] fl, StockObject so) {
		if (fl == null) {
			fl = new StockObject[2];
			fl[0] = so;
			fl[1] = so;
			return fl;
		}
		if (so.getDate().compareTo(fl[0].getDate()) < 0)
			fl[0] = so;
		if (so.getDate().compareTo(fl[1].getDate()) > 0)
			fl[1] = so;
		return fl;
	}

	public static double[] minMaxPrice(double[] minMax, double newPrice) {
		if(minMax == null) {
			minMax = new double[2];
			minMax[0]= newPrice;
			minMax[1] = newPrice;
			System.out.println("ERA NULLO, L'HO INIZIALIZZATO");
			System.out.println();
			return minMax;
		}
		else {
			if (newPrice < minMax[0])
				minMax[0] = newPrice;
			if (newPrice > minMax[1])
				minMax[1] = newPrice;
			return minMax;
		}
	}

	public static double mediumVolume(long sum, int k) {
		if(k == 0)
			return 0;
		double med = sum/k;
		return med;
	}

	public static double variationQuotation(Double first, Double last) {		
		double result = ((last.doubleValue() - first.doubleValue())/ first.doubleValue()) * 100;
		double roundOff = (double) ((Math.round(result * 100)) / 100);
		return roundOff;
	}
	
	public static Text firstOutput(double variation, double minimum,
			double maximum, double mediumVolume) {
		List<String> list = new ArrayList<String>();
		list.add(0, Double.toString(variation));
		list.add(1, Double.toString(minimum));
		list.add(2, Double.toString(maximum));
		list.add(3, Double.toString(mediumVolume));
		return ObjectSupports.listToText(list);
	}
	
	
	public static Map<Text, Double> sortByValues(Map<Text, Double> map) {
		Map<Text, Double> appoggio1= new HashMap<Text, Double>();

		Map<Double, Text> appoggio2 = new HashMap<Double, Text>();
		for (Text key : map.keySet()) {
			appoggio2.put(map.get(key), key);

		}
		Set<Double> a=(appoggio2.keySet());
		Double[] b= (Double[])a.toArray();
		Arrays.sort(b);
		for (Double key : b) {
			appoggio1.put(appoggio2.get(key), key);

		}

		return appoggio1;

	}

	public static Map<Text, Double> sorted_dasortare (Map<Text, JobOneOutOne> map) {
		Map<Text, Double> appoggio1= new HashMap<Text, Double>();

		Map<Double, Text> appoggio2 = new HashMap<Double, Text>();
		for (Text key : map.keySet()) {
			appoggio2.put(map.get(key).getVariation(), key);

		}

		for (Double key : appoggio2.keySet()) {

			appoggio1.put(appoggio2.get(key), key);

		}

		return appoggio1;

	}
}
