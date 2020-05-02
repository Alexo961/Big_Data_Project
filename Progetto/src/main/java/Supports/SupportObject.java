package Supports;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

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
		
		try {ticker = list.get(0);} catch (Exception exc) {ticker = null;}
		try {open = Double.parseDouble(list.get(1));} catch (Exception exc) {open = null;}
		try {close = Double.parseDouble(list.get(2));} catch (Exception exc) {close = null;}
		try {adj = Double.parseDouble(list.get(3));} catch (Exception exc) {adj = null;}
		try {low = Double.parseDouble(list.get(4));} catch (Exception exc) {low = null;}
		try {high = Double.parseDouble(list.get(5));} catch (Exception exc) {high = null;}
		try {volume = Long.parseLong(list.get(6));} catch (Exception exc) {volume = null;}
		try {date = LocalDate.parse(list.get(7), formatter);} catch (Exception exc) {date = null;}
		
		stock = new StockObject(ticker, open, close, adj, low, high, volume, date);
		return stock;
	}

}
