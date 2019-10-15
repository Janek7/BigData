import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Summer {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final Text word = new Text("sum");

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			while (itr.hasMoreTokens()) {
				context.write(word, new IntWritable(Integer.parseInt(itr.nextToken())));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 2)
			args = new String[] {"resources\\numbers.txt", "output\\summer\\sum" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime())};

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calculate sum");

		job.setJarByClass(Summer.class);
		job.setMapperClass(TokenizerMapper.class);
		
		// Diesmal mit dem Standard-IntSumReducer von Hadoop
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		System.exit(0);
	}

}
