package ucsc.hadoop.homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is writable comparable class encapsulates how to represent a composite
 * key used for partitioning, based upon movie count and actor names.
 * 
 */
public class MovieCntWritable implements WritableComparable<MovieCntWritable> {

	private IntWritable cnt;
	private Text actor;
	
	public MovieCntWritable() {
		cnt = new IntWritable();
		actor = new Text();
	}
	
	public IntWritable getMovieCnt() {
		return cnt;
	}
	
	public void setMovieCnt(IntWritable num) {
		cnt = num;
	}
	
	public Text getActorName() {
		return actor;
	}
	
	public void setActorName(Text name) {
		actor = name;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		cnt.write(out);
		actor.write(out);			
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cnt.readFields(in);
		actor.readFields(in);	
	}

	@Override
	public int compareTo(MovieCntWritable o) {
		int cntResult = (o.cnt).compareTo(this.cnt);
		
		if (cntResult !=0) {
			//count is not the same
			return cntResult;
		}	
		return (this.actor).compareTo(o.actor);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((actor == null) ? 0 : actor.hashCode());
		result = prime * result + ((cnt == null) ? 0 : cnt.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MovieCntWritable other = (MovieCntWritable) obj;
		if (actor == null) {
			if (other.actor != null)
				return false;
		} else if (!actor.equals(other.actor))
			return false;
		if (cnt == null) {
			if (other.cnt != null)
				return false;
		} else if (!cnt.equals(other.cnt))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MovieCntWritable [cnt=" + cnt + ", actor=" + actor + "]";
	}		
	
	
}	