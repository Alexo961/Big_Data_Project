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

public class Job1_2 extends Mapper<LongWritable, Text, Text, Text> {

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
			String settore_data = str[1]+ "_" + yearString;
			Text chiave = new Text(settore_data);
			context.write(chiave, value);
		}

		else {

			System.out.println("Riga spuria");
		}
	}



}
