package examexercises;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		@Override
		public void map(LongWritable inputKey, Text inputValue, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(inputValue.toString(), " .,;:!?#\"()-'*");
			while (itr.hasMoreTokens()) {
				context.write(new Text(itr.nextToken()), ONE);
			}
		}

	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : inputValues) {
				sum += intWritable.get();
			}
			context.write(inputKey, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		String inputFile = "resources\\faust.txt";
		String outputDir = "output\\examexercises\\word_count\\"
				+ new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime());

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count test");

		FileInputFormat.addInputPath(job, new Path(inputFile));
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(3);

		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		System.exit(0);

	}

}
