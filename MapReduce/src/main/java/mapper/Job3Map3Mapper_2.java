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
		String chiave = line.split("\t")[0];
		String valore =line.split("\t")[1];
		if(str.length == 4) {
			
			String yearString = chiave.split("_")[1];
			String variaz_quot =  valore.split("_")[1];
			String nome_azienda =  valore.split("_")[2];
		
			String keyless = nome_azienda;
			String anno_var = yearString + ": " + variaz_quot;
			context.write(new Text(keyless),new Text( anno_var));
		    

		}else {

			System.out.println("Riga spuria");
		}
	}



}
