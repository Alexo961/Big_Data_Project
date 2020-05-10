
package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import mapper.FirstJobFirstMapperFile;
import mapper.FirstJobSecondMapperFile;
import reducer.JoinReducer;
import support.*;


public class JoinDriver {

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {


		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	
	public static void main(String[] args)  throws Exception {

		if (args.length != 3) {
			System.out.println("Usage: <Primo file> <Secondo File> <output>");
			System.exit(0);;
		}

		//JobConf conf = new JobConf(getConf(), getClass());
		//conf.setJobName("Join 'Primo File csv' with 'Secondo File csv'");

		Job job = Job.getInstance( new Configuration(),"First join Job");

		job.setJarByClass(JoinDriver.class);

		Path AInputPath = new Path(args[0]);
		Path BInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, AInputPath, TextInputFormat.class, FirstJobFirstMapperFile.class);
		MultipleInputs.addInputPath(job, BInputPath, TextInputFormat.class, FirstJobSecondMapperFile.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}


	

	
}
