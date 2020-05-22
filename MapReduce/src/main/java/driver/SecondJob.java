package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import mapper.Job2Map2Mapper;
import mapper.SecondJobFirstMapper;
import mapper.SecondJobSecondMapper;
import reducer.Job2Redu2Reducer;
import reducer.SecondJobFirstReducer;
import reducer.SecondJobSecondReducer;

public class SecondJob {
	
	private static final String intermedPath = "output/intermedJob2";

	public static void main(String[] args) throws Exception {
		Job job_part_1 = Job.getInstance(new Configuration(),"Job Part One");
		
        job_part_1.setJarByClass(SecondJob.class);
		
		job_part_1.setMapperClass(SecondJobFirstMapper.class);
		job_part_1.setReducerClass(SecondJobFirstReducer.class);
		

		FileInputFormat.addInputPath(job_part_1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_part_1, new Path(intermedPath));

		job_part_1.setOutputKeyClass(Text.class);
		job_part_1.setOutputValueClass(Text.class);

		job_part_1.waitForCompletion(true);
		
		Job job_part_2 = Job.getInstance(new Configuration(), "Job Part Two");
		
		job_part_2.setJarByClass(SecondJob.class);
		
		job_part_2.setMapperClass(SecondJobSecondMapper.class);
		job_part_2.setReducerClass(SecondJobSecondReducer.class);
		
		FileInputFormat.addInputPath(job_part_2, new Path(intermedPath));
		FileOutputFormat.setOutputPath(job_part_2, new Path(args[1]));
		
		job_part_2.setOutputKeyClass(Text.class);
		job_part_2.setOutputValueClass(Text.class);
		
		job_part_2.waitForCompletion(true);
	}
}
