import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PageRank {

	public static class VectorMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] elements = value.toString().split("\t");

			System.out.println("......................... " + key.toString() + " - " + value.toString());
			context.write(new Text(elements[0]), new DoubleWritable(Double.parseDouble(elements[1])));
		}
	}

	public static class MatrixMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().equals(""))
				return;

			System.out.println("------------" +  key.toString() + " -> " + value.toString());
			String[] elements = value.toString().split("\t");
			context.write(new Text(elements[0] + "\t" + elements[1]), new DoubleWritable(Double.parseDouble(elements[2])));
		}

	}

	public static class Multiplier extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<DoubleWritable> it = values.iterator();

			// Werte sind aufsteigend nach Key sortiert
			// Der Wert des Vektors ist daher immer der erste
			double first = it.next().get();  // vector
			double second = 0;
			
			// nun wird über die Einträge der Matrix iteriert
			while (it.hasNext()) {
				second = it.next().get();
				System.out.println("+++++++++++++++++++++++++ " + key.toString() + ":  "+ first + " * " + second + " = " + (first*second));
				result.set(first * second);

				// nun brauchen wir den hinteren Teil des Keys
				String s = key.toString();
				s = s.substring(s.indexOf("\t")+1);
				key.set(s);
				context.write(key, result);
			}
		}
	}

	public static class PRReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			String s = key.toString();
			if (s.contains("\t")) {
				s = s.substring(0, s.indexOf("\t"));
				key.set(s);
			}

			context.write(key, result);

			System.out.println("rrrrrrrrrrrrrrrrrrrrrrrrrrrrrr " + key.toString() + " - " + result.toString());
		}
	}

	public static class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;

			for (DoubleWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

	// Secondary Sorting
	public static class MyPartitioner extends Partitioner<Text, DoubleWritable> {
		@Override
		public int getPartition(Text key, DoubleWritable value, int numberOfPartitions) {
			String k = key.toString();

			if (k.contains("\t")) 
				k = k.substring(0, k.indexOf("\t"));


			return Math.abs(k.hashCode() % numberOfPartitions);
		}
	}

	public static class MyGroupingComparator extends WritableComparator {
		public MyGroupingComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable wc1, WritableComparable wc2) {
			String s1 = ((Text)wc1).toString();
			String s2 = ((Text)wc2).toString();

			if (s1.contains("\t")) 
				s1 = s1.substring(0, s1.indexOf("\t"));

			if (s2.contains("\t")) 
				s2 = s2.substring(0, s2.indexOf("\t"));

			return s1.compareTo(s2);
		}
	}

	// ----------------

	public static void main(String[] args) throws Exception {
		
		String dateString = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime());
		args = new String[] {"resources\\pagerank\\mymatrix.txt", "resources\\pagerank\\myvector.txt", 
				"output\\pagerank\\pr" + dateString + "\\output1", 
				"output\\pagerank\\pr" + dateString + "\\output2", 
				"output\\pagerank\\pr" + dateString + "\\output-final"};

		for (int i = 0; i < 1; i++) {
			Configuration conf = new Configuration();
			conf.set("length", "4");
			Job job = Job.getInstance(conf, "pr1");

			job.setJarByClass(PageRank.class);

			job.setNumReduceTasks(2);
			job.setPartitionerClass(MyPartitioner.class);
			job.setGroupingComparatorClass(MyGroupingComparator.class);
			job.setReducerClass(Multiplier.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, VectorMapper.class);

			Path outputPath = new Path(args[2]);
			FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath, true);

			job.waitForCompletion(true);

			// Job 2
			conf = new Configuration();
			job = Job.getInstance(conf, "pr2");

			job.setJarByClass(PageRank.class);
			job.setMapperClass(Mapper.class);
			job.setReducerClass(PRReducer.class);
			job.setNumReduceTasks(2);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, outputPath);
			job.setInputFormatClass(SequenceFileInputFormat.class);

			outputPath = new Path(args[3]);
			FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath, true);

			job.waitForCompletion(true);

			args[1] = "/tmp/output2-pr/";
		}

		// Sortieren
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "pr-sort");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(VectorMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[3]));

		Path outputPath = new Path(args[4]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		System.exit(0);

	}
}