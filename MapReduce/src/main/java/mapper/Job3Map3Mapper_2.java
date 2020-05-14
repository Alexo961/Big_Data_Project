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

public class Job3Map3Mapper_2 extends Mapper<LongWritable, Text, Text, Text> {

	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		LocalDate date;

		String line = value.toString();
		String[] str = line.split("_");
		if(str.length == 3) {
			
			String yearString = str[0];
			String variaz_quot = str[1];
			String nome_azienda = str[2];
		
			String chiave = nome_azienda +"_"+ variaz_quot;
			context.write(new Text(chiave),new Text( yearString));
		    

		}else {

			System.out.println("Riga spuria");
		}
	}



}
