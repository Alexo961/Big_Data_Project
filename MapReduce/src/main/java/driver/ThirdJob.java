package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.SecondJobFirstMapper;
import mapper.SecondJobSecondMapper;
import mapper.ThirdJobFirstMapper;
import mapper.ThirdJobSecondMapper;
import mapper.ThirdJobThirdMapper;
import reducer.SecondJobFirstReducer;
import reducer.SecondJobSecondReducer;
import reducer.ThirdJobFirstReducer;
import reducer.ThirdJobSecondReducer;
import reducer.ThirdJobThirdReducer;

public class ThirdJob {

	private static final String intermedPath1 = "output/intermed1Job3";
	private static final String intermedPath2 = "output/intermed2Job3";

	public static void main(String[] args) throws Exception {
		Job job_part_1 = Job.getInstance(new Configuration(),"Job Part One");

		job_part_1.setJarByClass(ThirdJob.class);

		job_part_1.setMapperClass(ThirdJobFirstMapper.class);
		job_part_1.setReducerClass(ThirdJobFirstReducer.class);


		FileInputFormat.addInputPath(job_part_1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_part_1, new Path(intermedPath1));

		job_part_1.setOutputKeyClass(Text.class);
		job_part_1.setOutputValueClass(Text.class);

		job_part_1.waitForCompletion(true);

		Job job_part_2 = Job.getInstance(new Configuration(), "Job Part Two");

		job_part_2.setJarByClass(ThirdJob.class);

		job_part_2.setMapperClass(ThirdJobSecondMapper.class);
		job_part_2.setReducerClass(ThirdJobSecondReducer.class);

		FileInputFormat.addInputPath(job_part_2, new Path(intermedPath1));
		FileOutputFormat.setOutputPath(job_part_2, new Path(intermedPath2));

		job_part_2.setOutputKeyClass(Text.class);
		job_part_2.setOutputValueClass(Text.class);

		job_part_2.waitForCompletion(true);

		Job job_part_3 = Job.getInstance(new Configuration(), "Job Part Three");

		job_part_3.setJarByClass(ThirdJob.class);

		job_part_3.setMapperClass(ThirdJobThirdMapper.class);
		job_part_3.setReducerClass(ThirdJobThirdReducer.class);

		FileInputFormat.addInputPath(job_part_3, new Path(intermedPath2));
		FileOutputFormat.setOutputPath(job_part_3, new Path(args[1]));

		job_part_3.setOutputKeyClass(Text.class);
		job_part_3.setOutputValueClass(Text.class);

		job_part_3.waitForCompletion(true);
	}
}
