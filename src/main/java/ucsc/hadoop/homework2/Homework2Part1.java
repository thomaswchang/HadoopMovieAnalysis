package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;

/**
 * Part #1: Develop a MapReduce application to show which actors played in each
 * movie. The input data is imdb.tsv. The output data would have two columns:
 * movie title and a semicolon separated list of actor names.
 * 
 * Each record in imdb.tsv represents actor, movie title, and year
 * 
 * To execute the job, run the runHw1.sh
 */
public class Homework2Part1 extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Homework2Part1.class);	
	
	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part1.class.getName());

		int exitCode = ToolRunner.run(new Homework2Part1(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Usage: Homework2Part1 <input directory> <out directory>";
			System.err.println(msg);
			System.exit(2);
		}
		
		// Class encapsulates Hadoop configuration settings
		Configuration conf = getConf();
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.debug("input directory: " + args[0] + " output directory: " + args[1]);
		
		Job job = new Job(conf, "Homework 2 Part 1: movie and actors list");
		job.setJarByClass(Homework2Part1.class);
		job.setMapperClass(MovieTitleAndActorsMapper.class);
		job.setReducerClass(MovieTitleAndActorsReducer.class);
		
		// Set intermediate values types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set job output types 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Specify directory for input and output for job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		// Kick off job and wait till job is done
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;		
	}
	
	private static class MovieTitleAndActorsMapper extends Mapper<Object, Text, Text, Text> {
	
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Object key is a byte offset to the input file. Will not use.			
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				String movieName = tokens[1];
				String actorName = tokens[0];
				context.write(new Text(movieName), new Text(actorName));
			}
		}
	}
	
	private static class MovieTitleAndActorsReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		public void reduce(Text movieName, Iterable<Text> actorNames, Context context) throws IOException, InterruptedException {

			StringBuffer names = new StringBuffer();			
			for (Text name : actorNames) {
				String s[] = name.toString().split("\\s*,\\s*");
				LOG.info(s);
				if (s.length == 2) {
					names.append(s[1] + " ");
					names.append(s[0]);
					names.append(", ");
				}
			}
			names.append("\n");
			
			context.write(movieName, new Text(names.toString()));
		}
	}
}
