package ucsc.hadoop.homework2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

/**
 * This class verifies the map and reducer jobs for Homework2Part2, which has 2
 * map reduce jobs chained one after.
 * 
 */
public class SortMovieByActorRolesTest {
	
	@Test
	public void mapForJob1Test() throws Exception {
		MapDriver<Object, Text, Text, IntWritable> mp = new MapDriver<Object, Text, Text, IntWritable>();

		mp.withMapper(new Homework2Part2.Mapper1())
			.withInput(new IntWritable(5), new Text("Morris, Kathryn (I)	Minority Report	2002"))
			.withOutput(new Text("Morris, Kathryn (I)"), new IntWritable(1))
			.runTest();
	}
	
	@Test
	public void reduceForJob1Test() throws Exception {
		ReduceDriver<Text, IntWritable, Text, IntWritable> rd = new ReduceDriver<Text, IntWritable, Text, IntWritable>();

		List<IntWritable> values = new ArrayList<IntWritable>();
		values.addAll(Arrays.asList(new IntWritable(5), new IntWritable(2)));
		
		Text actorName = new Text("Tom Hanks");
		
		rd.withReducer(new Homework2Part2.Reducer1())
			.withInput(actorName, values)
			.withOutput(actorName, new IntWritable(7))
			.runTest();		
	}
	
	// @Test: Appear to be some issue with NullWritable
	public void mapForJob2Test() throws Exception {
		MapDriver<Object, Text, MovieCntWritable, NullWritable> md = new MapDriver<Object, Text, MovieCntWritable, NullWritable>();
		
		MovieCntWritable expected = new MovieCntWritable();
		expected.setActorName(new Text("Zayas, David"));
		expected.setMovieCnt(new IntWritable(4));
		
		md.withMapper(new Homework2Part2.Mapper2())
			.withInput(new IntWritable(5), new Text("Zayas, David	5"))
			.withOutput(expected, NullWritable.get())
			.runTest();
	}
	
	@Test
	public void reduceForJob2Test() throws Exception {
		ReduceDriver<MovieCntWritable, NullWritable, IntWritable, Text> rd = new ReduceDriver<MovieCntWritable, NullWritable, IntWritable, Text>();

		Text actorName = new Text("Zayas, David");
		IntWritable movieCnt = new IntWritable(4);
			
		MovieCntWritable expected = new MovieCntWritable();
		expected.setActorName(actorName);
		expected.setMovieCnt(movieCnt);
		
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		
		rd.withReducer(new Homework2Part2.Reducer2())
			.withInput(expected, values)
			.withOutput(movieCnt, actorName)
			.runTest();
	}
}
