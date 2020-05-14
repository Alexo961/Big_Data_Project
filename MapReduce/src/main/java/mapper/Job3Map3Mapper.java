package mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import objects.StockObject;

public class Job3Map3Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text ticker;
	private static final String DATE_PATTERN = "yyyy-MM-dd";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		LocalDate date;

		String line = value.toString();
		String[] str = line.split(",");
		if(str.length == 9) {
			date = LocalDate.parse(str[8], DATE_TIME_FORMATTER);
			Integer year = date.getYear();
			String yearString = year.toString();
			
			
			String nome = str[0].split("\t")[1];
		
			String ticker = str[0].split("\t")[0];
			
			String ticker_data = ticker+ "_" + yearString;
			String quotazione = String.valueOf((Double.parseDouble(str[4]) - Double.parseDouble(str[3])));//differenza tra prezzo di apertura e chiusura azione
			Text chiave = new Text(ticker_data);
			
			Text valore = new Text(ticker+"_" +nome +"_"+ quotazione+"_"+date.toString());//ticker_nomeazienda_quotazione_date
			
			if(yearString.equals("2018")  || yearString.equals("2017") || yearString.equals("2016")) 
			context.write(chiave, valore);
		    

		}else {

			System.out.println("Riga spuria");
		}
	}



}
