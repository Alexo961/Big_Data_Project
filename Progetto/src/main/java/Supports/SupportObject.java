package Supports;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class SupportObject {


	//MIAOBAUCHICCHIRICHI
	private Double ogettoDouble ;
	private LocalDate ogettoDate;

	private static String pattern = "yyyy-MM-dd";
	private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

	public SupportObject(Double ogettoDouble, LocalDate ogettoDate) {
		super();
		this.ogettoDouble = ogettoDouble;
		this.ogettoDate = ogettoDate;
	}

	public Double getOgettoDouble() {
		return ogettoDouble;
	}

	public void setOgettoDouble(Double ogettoDouble) {
		this.ogettoDouble = ogettoDouble;
	}

	public LocalDate getOgettoDate() {
		return ogettoDate;
	}

	public void setOgettoDate(LocalDate ogettoDate) {
		this.ogettoDate = ogettoDate;
	}

	public static StockObject transform(List<String> list) {
		StockObject stock;
		String pippo;
		Double open;
		Double close;
		Double adj;
		Double low;
		Double high;
		Long volume;
		LocalDate date;

		try {
			pippo = list.get(0);
			open = Double.parseDouble(list.get(1));
			close = Double.parseDouble(list.get(2));
			adj = Double.parseDouble(list.get(3));
			low = Double.parseDouble(list.get(4));
			high = Double.parseDouble(list.get(5));
			volume = Long.parseLong(list.get(6));
			date = LocalDate.parse(list.get(7), formatter);
		}
		catch (Exception exc) {
			return null;
		}

		stock = new StockObject(pippo, open, close, adj, low, high, volume, date);
		return stock;
	}

	public static StockObject[] first_last(StockObject[] fl, StockObject so) {
		if (fl[0] == null && fl[1] == null) {
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


	public static Double[] min_max( Double[] fl, double p1) {
		if(fl[0] == null && fl[1]== null) {
			fl[0]= p1;
			fl[1] = p1;
			return fl;
		}else{

			if (p1 < (fl[0].doubleValue()))
				fl[0] = p1;
			if (p1 >(fl[1].doubleValue()))
				fl[1] = p1;
			return fl;
		}
	}
	//ritorna il valore medio della quotaz. inserendo la somma dei valori medi e il numero di valori per ogni ticker
	public static Double vol_med(long sum, int k) {

		double med = sum/k;
		Double valore = new Double(med);

		return valore;

	}



	public static LocalDate randomDate() {
		long minDay = LocalDate.of(2008, 1, 1).toEpochDay();
		long maxDay = LocalDate.of(2018, 12, 31).toEpochDay();
		long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
		LocalDate randomDate = LocalDate.ofEpochDay(randomDay);
		return randomDate;
	}

	public static Double variationquot(Double iniz,Double finale) {
		Double variation;
		try {

			double result = ((finale-iniz)/ iniz) * 100;
			double roundOff = (double) (Math.round(result * 100) / 100);
			variation = new Double(result);


		}catch(Exception e) {
			variation = new Double(0.0);
			System.out.println("NON FUNZIONA!");
		}

		return variation;
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

	public static Map<Text, Double> sorted_dasortare (Map<Text, OutputObject> map) {
		Map<Text, Double> appoggio1= new HashMap<Text, Double>();

		Map<Double, Text> appoggio2 = new HashMap<Double, Text>();
		for (Text key : map.keySet()) {
			appoggio2.put(map.get(key).getVariazione(), key);

		}

		for (Double key : appoggio2.keySet()) {

			appoggio1.put(appoggio2.get(key), key);

		}

		return appoggio1;

	}

}
