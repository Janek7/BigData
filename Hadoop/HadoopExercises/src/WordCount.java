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
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " .,;:!?#\"()-'*");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	// nur zur Demonstration, den gibt es auch von Hadoop schon vordefiniert
	public static class MyIntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private int total = 0;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);

			total += sum;
		}

	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 2)
			args = new String[] {"resources\\faust.txt", "output\\word_count\\wc" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime())};

		//System.setProperty("hadoop.home.dir", "C:\\hadoop-2.8.5");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MyIntSumReducer.class);
		job.setReducerClass(MyIntSumReducer.class);
		job.setNumReduceTasks(4);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		System.exit(0);
	}

}