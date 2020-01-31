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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

public class WordCountSortedGlobally {

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
	
	public static class SwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
		
		@Override
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			if (value.get() > 1) context.write(value, key); // filter long tail distribution
		}
		
	}

	public static class DescendingComparator extends WritableComparator {

		public DescendingComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -1 * super.compare(a, b);
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String inputFile = "resources\\faust.txt";
		String outputDir = "output\\examexercises\\word_count_sorted_globally\\" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime()) + "\\";
		String outputDirCount = outputDir + "count";
		String outputDirSwitch = outputDir + "switch";
		String outputDirSorted = outputDir + "sorted";

		// Count Job
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count");
		FileInputFormat.addInputPath(job, new Path(inputFile));
		
		job.setJarByClass(WordCountSortedGlobally.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);

		FileOutputFormat.setOutputPath(job, new Path(outputDirCount));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
		
		
		// Switch Job
		conf = new Configuration();
		job = Job.getInstance(conf, "switch");
		FileInputFormat.addInputPath(job, new Path(outputDirCount));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setJarByClass(WordCountSortedGlobally.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputDirSwitch));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
		
		
		// Sort Job
		conf = new Configuration();
		job = Job.getInstance(conf, "sort");
		FileInputFormat.addInputPath(job, new Path(outputDirSwitch));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setJarByClass(WordCountSortedGlobally.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setNumReduceTasks(4);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// apply sorting with modified partitioning
		job.setSortComparatorClass(DescendingComparator.class);
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.05, 1000, 4);
		InputSampler.writePartitionFile(job, sampler);
		TotalOrderPartitioner.setPartitionFile(conf, new Path("temp/partition"));
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputDirSorted));
		job.waitForCompletion(true);

	}

}
