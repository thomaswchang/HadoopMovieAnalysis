package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import ucsc.hadoop.util.ConfigurationUtil;


/**
 * Class represent a map reduce job that process a movie list containing 
 * 	-movie title
 * 	-actor name, 
 * 	-production year
 * 
 * and returns the number of times an actor was in a movie, sorted in 
 * descending order. 
 * 
 * To execute the job, run the runHw2.sh
 * 
 */
public class Homework2Part2 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part2.class.getName());		
		
		int exitCode = ToolRunner.run(new Homework2Part2(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Usage: Homework2Part1 <input directory> <out directory>";
			System.err.println(msg);
			System.exit(2);
		}
		
		Configuration conf = getConf();
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		Job job1 = new Job(conf, "Homework 2 Part2: returns the number of times an actor was in a move");
		job1.setJarByClass(Homework2Part2.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		String intermediateDir = args[1] + "_tmp";
		FileOutputFormat.setOutputPath(job1, new Path(intermediateDir));				
		boolean result = job1.waitForCompletion(true);

		Job job2 = new Job(conf, "part 2");
		job2.setJarByClass(Homework2Part2.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(MovieCntWritable.class);
		job2.setMapOutputValueClass(NullWritable.class);	
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setPartitionerClass(MoviePartitioner.class);				
		FileInputFormat.addInputPath(job2, new Path(intermediateDir));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		result |= job2.waitForCompletion(true);
		
		return (result) ? 0 : 1;
	}


	// Given a key value pair denoting an offset to the input file and the line
	// of text, Mapper1 returns a key value pair of actor name to count
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				String actorName = tokens[0];
				context.write(new Text(actorName), ONE);
			}			
		}		
	}
	
	
	
	// Why are reduce key ordered? Because there are multiple reducers. Each
	// reducer gets a unique set of keys that no other reducer gets. This is
	// also why the all the reducer has to wait until ALL the mappers are done.
	// This enables the reducer to be INDEPENDANT of other reduce jobs. This is
	// also the reason why I could not combine this assignment into one
	// map-reduce job. Movie count is not "unique".
	
	// Given an intermediate key value pair of actor and cnt, the reducer sums
	// the movie count for each actor.
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int movieCnt = 0;
			for (IntWritable each : values) {
				movieCnt += each.get();
			}
			context.write(key, new IntWritable(movieCnt));
		}
	}
	
	// Note1: made an error here: mapper inputs are always index to the file
	// offset, and value is the entire line
	//
	// Note 2: So how did I achieve ordering the result by movie count?
	// (1) Define my own MovieCntWritable, which implements the writable
	// comparable. My comparable is based on the composite key of movie cnt and
	// actor name
	// (2) Define a partitioner class, that uses the MovieCntWritable
	// (3) Set the job partitionclass to use the partitioner class
	public static class Mapper2 extends Mapper<Object, Text, MovieCntWritable, NullWritable> {

		@Override
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length ==2) {
				String actorName = tokens[0];			
				int movieCnt = new Integer(tokens[1]);
				
				MovieCntWritable m = new MovieCntWritable();
				m.setActorName(new Text(actorName));
				m.setMovieCnt(new IntWritable(movieCnt));
				context.write(m, NullWritable.get());
			}
		}
	}
	
	// - Class is used by the partitoner class, which uses the MovieCntWritable's
	// comparable function to partition the intermediate keys to the reduce job.
	// - The generic types matches the mapper out (ie mapper2)
	public static class MoviePartitioner extends Partitioner<MovieCntWritable, NullWritable> {
		@Override
		public int getPartition(MovieCntWritable key, NullWritable value,
				int numPartitions) {
			return (Math.abs(key.getMovieCnt().get() * 127) % numPartitions);
		}
	}

	// Reduce takes the intermediate key value MovieCntWritable, NullWritable,
	// and returns the count and the actor name.
	public static class Reducer2 extends Reducer<MovieCntWritable, NullWritable, IntWritable, Text> {
		@Override
		public void reduce(MovieCntWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			IntWritable cnt = key.getMovieCnt();
			Text name = key.getActorName();
			context.write(cnt, name);
		}		
	}		
}