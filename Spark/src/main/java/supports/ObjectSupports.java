package supports;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;

import objects.ActionObject;
import objects.SectorObject;
import objects.StockObject;

public class ObjectSupports {

	private static final String DATE_PATTERN = "yyyy-MM-dd";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);
	private static final int STOCK_NUM_FIELDS = 8;
	private static final int SECTOR_NUM_FILEDS = 5;

	public static ActionObject stringToActionObject(String line) {
		List<String> list;
		list = stringToList(line);
		if (list.size() == STOCK_NUM_FIELDS) {
			return listToStockObject(list);
		}
		if (list.size() == SECTOR_NUM_FILEDS)
			return listToSectorObject(list);
		return null;
	}

	public static List<String> actionObjectToList(ActionObject ao){
		try {
			int num = ao.getNumFields();
			if (num == SECTOR_NUM_FILEDS)
				return sectorObjectToList((SectorObject)ao);
			if (num == STOCK_NUM_FIELDS)
				return stockObjectToList((StockObject)ao);
			return null;
		}
		catch(Exception exc) {
			return null;
		}
	}

	public static StockObject listToStockObject(List<String> list) {
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
			if (list.size() == STOCK_NUM_FIELDS)
			{
				ticker = list.get(0);
				open = Double.parseDouble(list.get(1));
				close = Double.parseDouble(list.get(2));
				adj = Double.parseDouble(list.get(3));
				low = Double.parseDouble(list.get(4));
				high = Double.parseDouble(list.get(5));
				volume = Long.parseLong(list.get(6));
				date = LocalDate.parse(list.get(7), DATE_TIME_FORMATTER);
			}
			else
				return null;
		}
		catch (Exception exc) {
			return null;
		}

		stock = new StockObject(ticker, open, close, adj, low, high, volume, date);
		return stock;
	}

	public static List<String> stockObjectToList(StockObject so){
		try {
			String[] array = new String[STOCK_NUM_FIELDS];
			array[0] = so.getTicker().toString();
			array[1] = so.getOpen().toString();
			array[2] = so.getClose().toString();
			array[3] = so.getAdj().toString();
			array[4] = so.getLow().toString();
			array[5] = so.getHigh().toString();
			array[6] = so.getVolume().toString();
			array[7] = so.getDate().format(DATE_TIME_FORMATTER);
			return Arrays.asList(array);
		}
		catch(Exception exc) {
			return null;
		}
	}

	public static List<String> sectorObjectToList(SectorObject so){
		try {
			String[] array = new String[SECTOR_NUM_FILEDS];
			array[0] = so.getTicker();
			array[1] = so.getExchange();
			array[2] = so.getName();
			array[3] = so.getSector();
			array[4] = so.getIndustry();
			return Arrays.asList(array);
		}
		catch (Exception exc) {
			return null;
		}
	}

	public static Text listToText(List<String> list) {
		Iterator<String> it = list.iterator();
		StringBuilder sb = new StringBuilder("");
		if (!it.hasNext()) {
			return new Text("");
		}
		else {
			while(it.hasNext()) {
				sb.append(it.next());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			String line = sb.toString();
			return new Text(line);
		}
	}

	public static String StringToText(String[] str, int lim1, int lim2) {
		String finale = new String();
		for(int i=0 ; i< str.length; i++) {
			if(i!=lim1 && i!=lim2)
				finale =	finale.concat(str[i]+",");

		}
		finale = finale.substring(0, finale.length() - 1);

		return finale;
	}

	public static List<String> stringToList(String line){
		List<String> list = new ArrayList<>();
		String[] array = line.split(",");
		StringBuilder sb = new StringBuilder();
		boolean containsComma = false;
		if(array.length > 0) {
			for (String string : array) {
				if (string.charAt(0) == '"') {
					containsComma = true;
					sb.append(string).append(",");
					continue;
				}
				if (containsComma) {
					sb.append(string).append(",");
					if (string.charAt(string.length() - 1) == '"') {
						containsComma = false;
						list.add(sb.toString());
					}
					continue;
				}
				list.add(string);
			}
			return list;
		}
		return null;
	}

	public static SectorObject listToSectorObject(List<String> list) {
		SectorObject so;
		String ticker;
		String exchange;
		String name;
		String sector;
		String industry;
		try {
			if (list.size() == SECTOR_NUM_FILEDS) {
				ticker = list.get(0);
				exchange = list.get(1);
				name = list.get(2);
				sector = list.get(3);
				industry = list.get(4);
				so = new SectorObject(ticker, exchange, name, sector, industry);
				return so;
			}
			else
				return null;
		}
		catch(Exception exc) {
			return null;
		}
	}

	public static Text actionObjectToText(ActionObject ao) {
		List<String> list = actionObjectToList(ao);
		return listToText(list);
	}

}
