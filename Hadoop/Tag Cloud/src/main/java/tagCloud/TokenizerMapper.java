package tagCloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while (stringTokenizer.hasMoreTokens()) {
			word.set(stringTokenizer.nextToken());
			context.write(word, ONE);
		}
		
	}

}
