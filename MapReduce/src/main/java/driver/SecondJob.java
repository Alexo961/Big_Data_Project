package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import mapper.Job2Map2Mapper;

import reducer.Job2Redu2Reducer;

public class SecondJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance( new Configuration(),"Second Job");
		
        job.setJarByClass(SecondJob.class);
		
		job.setMapperClass(Job2Map2Mapper.class);
		job.setReducerClass(Job2Redu2Reducer.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}
