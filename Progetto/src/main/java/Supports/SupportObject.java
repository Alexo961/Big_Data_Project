package Supports;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class SupportObject {
	
	
	private Double ogettoDouble ;
	private LocalDate ogettoDate;
	
	private static String pattern = "yyyy/MM/dd";
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
		String ticker;
		Double open;
		Double close;
		Double adj;
		Double low;
		Double high;
		Long volume;
		LocalDate date;
		
		try {
			ticker = list.get(0);
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
		
		stock = new StockObject(ticker, open, close, adj, low, high, volume, date);
		return stock;
	}
	
	public static StockObject[] first_last(StockObject[] fl, StockObject so) {
			
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
	public static Double vol_med(double sum, int k) {
		
		Double valore = 0.0;
		
		valore =sum/k;
		
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
		Double result=(double) 0;
		try {
		
	    result = ((finale-iniz)/ iniz) * 100;
	   double roundOff = (double) Math.round(result * 100) / 100;
	   result = roundOff;
		
		
		}catch(Exception e) {
		
		
		
		
		
		}
		
		return result;
	}

}
