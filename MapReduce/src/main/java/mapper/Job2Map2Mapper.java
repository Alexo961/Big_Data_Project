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

public class Job2Map2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text ticker;
	private static final String DATE_PATTERN = "yyyy-MM-dd";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		LocalDate date;

		String line = value.toString();
		String[] str = line.split(",");
		if(str.length == 10) {
			String tickerAndName = str[0] + "," + str[1];
			String newStr[] = new String[9];
			newStr[0] = tickerAndName;
			for (int j = 1; j < 9; j++) {
				newStr[j] = str[j+1];
			}
			str = newStr;
		}
		if(str.length == 9) {
			date = LocalDate.parse(str[8], DATE_TIME_FORMATTER);
			Integer year = date.getYear();
			String yearString = year.toString();
			
			String nome = str[0].split("\t")[1];
			String ticker = str[0].split("\t")[0];
			String settore_data = str[1]+ "_" + yearString;
			String quotazione = String.valueOf((Double.parseDouble(str[4]) - Double.parseDouble(str[3])));
			Text chiave = new Text(settore_data);
			Text valore = new Text(str[7]+"_"+str[3]+"_"+str[4]+"_"+str[8]+"_" +nome +"_"+ quotazione+"_"+ticker);//volume_open_close_data_nomeazienda_quotazione
			context.write(chiave, valore);
		}

		else {

			System.out.println("Riga spuria");
		}
	}



}
