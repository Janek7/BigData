import java.io.IOException;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ReflectionUtils;

public class WordCountStatistik {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			
			if (s.startsWith("#") || s.equals(":")) // Kommentar
				return;
			else if (s.endsWith(":") && Character.isUpperCase(s.charAt(0))) {  // Charaktere
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < s.length(); i++) {
					char c = s.charAt(i);
					if (Character.isUpperCase(c) || c == ' ' || c == '-')
						sb.append(c);
					else if (i > 1) {

						word.set("Auftritt(e) von " + sb.toString().substring(0, sb.toString().length()));
						context.write(word, one);
						s = s.substring(i, s.length());

						break;
					} else 
						break;
				}
			}

			// Normaler WordCount für sonstigen Text, auch Auftritte werden nochmal mitgezählt
			StringTokenizer itr = new StringTokenizer(s, " .,;:!?#\"()-'*");
			while (itr.hasMoreTokens()) {
				s = itr.nextToken().toLowerCase();
				word.set(s);
				context.write(word, one);
			}
		}
	}

	public static class SwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
		@Override
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			if (value.get() > 1) // Seltene Worte wg. LongTail ausfiltern, > 1 reicht hier schon
				context.write(value, key);
			else
				context.getCounter("stats", "filtered").increment(value.get());
			
			context.getCounter("stats", "words").increment(value.get());
		}
	}
	
	public static class MyDescendingComparator extends WritableComparator {

		public MyDescendingComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return super.compare(a, b) * -1;
		}

	}


	public static void main(String[] args) throws Exception {
		
		String timestamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime());
		args = new String[] {"resources\\faust.txt", 
				"output\\word_count_stats\\count\\count_" + timestamp, 
				"output\\word_count_stats\\swapped\\swapped_" + timestamp, 
				"output\\word_count_stats\\final_sorted\\final_sorted_" + timestamp};

		// Job1: Tokenisieren und als SequenceFile schreiben
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count tokenizing");

		job.setJarByClass(WordCountStatistik.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(4);

		// nur notwendig, wenn sich die Output-Typen bei Mapper und Recucer unterscheiden
		// vgl.: https://stackoverflow.com/questions/38376688/why-setmapoutputkeyclass-method-is-necessary-in-mapreduce-job
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);

		// ------------------------

		// Job2: Key und Value vertauschen
		conf = new Configuration();
		job = Job.getInstance(conf, "word count switch");

		job.setJarByClass(WordCountStatistik.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(4);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		System.out.println("Gesamtzahl der Worte: " +  job.getCounters().findCounter("stats", "words").getValue());
		long deleted = job.getCounters().findCounter("stats", "filtered").getValue();

		// --------------------------------------------

		// Job3: Final wird jetzt noch nach Anzahl sortiert, das passiert mit leerem Mapper und Reducer automatisch
		// wichtig ist das Routing über den Partitioner unten

		conf = new Configuration();
		job = Job.getInstance(conf, "word count rank");

		job.setJarByClass(WordCountStatistik.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(4);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setSortComparatorClass(MyDescendingComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// Das ist der spannende Teil!
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/tmp/pout"));
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.05, 1000, 4);
		InputSampler.writePartitionFile(job, sampler);
		job.setPartitionerClass(TotalOrderPartitioner.class);

		outputPath = new Path(args[3]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		System.out.println("Anzahl verschiedener Worte: " + job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue() );
		System.out.println("+ Gelöschte Worte aus dem Long Tail: " +  deleted);

		System.exit(0);
	}

}
