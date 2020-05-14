package support;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

import objects.StockObject;
import outputobjects.JobOneOutOne;

public class JobSupports {
	
	private static final String DATE_PATTERN = "yyyy-MM-dd";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);


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
			System.out.println("AGGIORNO MINMAX");
			if (newPrice < minMax[0]) {
				minMax[0] = newPrice;
				System.out.println(minMax[0] + " " + minMax[1]);
			}
			if (newPrice > minMax[1]) {
				System.out.println(minMax[0] + " " + minMax[1]);
				minMax[1] = newPrice;
			}
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
	
	public static Text outOneToText(JobOneOutOne output) {
		List<String> list = new ArrayList<>();
		list.add(0, Double.toString(output.getVariation()));
		list.add(1, Double.toString(output.getMinimum()));
		list.add(2, Double.toString(output.getMaximum()));
		list.add(3, Double.toString(output.getMeanVolume()));
		return ObjectSupports.listToText(list);
	}
	
	
	public static Map<Text, Double> sortByValues(Map<Text, Double> map) {
		Map<Text, Double> appoggio1= new HashMap<Text, Double>();

		Map<Double, Text> appoggio2 = new HashMap<Double, Text>();
		for (Text key : map.keySet()) {
			appoggio2.put(map.get(key), key);

		}
		Set<Double> a=(appoggio2.keySet());
		List<Double> b = new ArrayList<Double>(a);
		Collections.sort(b);
		for (Double key : b) {
			appoggio1.put(appoggio2.get(key), key);

		}

		return appoggio1;

	}

	public static Map<Text, Double> sorted_dasortare (Map<Text, JobOneOutOne> map) {
		Map<Text, Double> appoggio1= new HashMap<Text, Double>();
		System.out.println("MAPPA INPUT:");
		for (Text key : map.keySet()) {
			System.out.println(key.toString());
			System.out.println(outOneToText(map.get(key)).toString());
		}

		Map<Double, Text> appoggio2 = new HashMap<Double, Text>();
		for (Text key : map.keySet()) {
			System.out.println("APPOGGIO1");
			System.out.println(map.size());
			System.out.println("DENTRO APPOGGIO 2 METTO:");
			System.out.println(map.get(key).getVariation().toString());
			System.out.println(key.toString());
			appoggio2.put(map.get(key).getVariation(), key);

		}

		for (Double key : appoggio2.keySet()) {
			
			System.out.println("APPOGGIO2");
			System.out.println(appoggio2.size());
			appoggio1.put(appoggio2.get(key), key);

		}

		return appoggio1;

	}
	
	
	
	public  static Double medVolAnn (int k , Double sum) {
		return sum/k;
		
	}
	public  static Double medquotAnn (int k , Double sum) {
		return sum/k;
		
	}
	
	
	public static Map<Text,Double>  media_per_ticker(Double quotazione, Text ticker , Map<Text,Double> map) {
		
		if(!map.containsKey(ticker)) {
			
			map.put(ticker, quotazione);
			
		}else {
			
			Double value = map.get(ticker).doubleValue();
		    map.put(ticker,(value + quotazione) )	;
			
		}
		
		
		return map;
		
	}
	
	public static Text stampa_mappa(Map<Text,Double> map) {
		StringBuilder sb = new StringBuilder();
		String sector;
		String year;
		for (Text key : map.keySet()) {
			sector = key.toString().split("_")[0];
			year = key.toString().split("_")[1];
			sb.append(sector)
			.append(",")
			.append(year)
			.append(": ")
			.append(map.get(key));
		}
		return new Text(sb.toString());
	}
	
	public static String[] firstLast(String[] fl, String line) {
		if (fl == null) {
			fl = new String[2];
			fl[0] = line;
			fl[1] = line;
			return fl;
		}
		LocalDate first, last, newDate;
		first = LocalDate.parse(fl[0].split("_")[3],DATE_TIME_FORMATTER);
		last = LocalDate.parse(fl[1].split("_")[3],DATE_TIME_FORMATTER);
		newDate = LocalDate.parse(line.split("_")[3],DATE_TIME_FORMATTER);
		if (first.compareTo(newDate) > 0)
			fl[0] = line;
		if (last.compareTo(newDate) < 0)
			fl[1] = line;
		return fl;
	}
	
	public static Double variationAnnualQuotation2(String[] fl) {
		Double first = Double.parseDouble(fl[0].split("_")[5]);
		Double last = Double.parseDouble(fl[1].split("_")[5]);
		Double result = ((last - first)/first *100);
		return result;
	}
	public static Double variationAnnualQuotation3(String[] fl) {
		if (fl == null)
			System.out.println("FL NULLO");
		else if (fl[0] == null || fl[1] == null ) {
			System.out.println("UNO DEI DUE è NULLO");
			if (fl[0] != null)
				System.out.println(fl[0]);
			else 
				System.out.println("FL0 NULLO");
			if (fl[1] != null)
				System.out.println(fl[1]);
			else
				System.out.println("FL1 NULLO");
		}
		else {
			System.out.println(fl[0]);
			System.out.println(fl[1]);
		}
		Double first = Double.parseDouble(fl[0].split("_")[2]);
		Double last = Double.parseDouble(fl[1].split("_")[2]);
		Double result = ((last - first)/first *100);
		return result;
	}
	
	
}
