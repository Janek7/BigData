package examexercises;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortingWritable {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString(), ", ");

			String ym = st.nextToken() + "-" + st.nextToken();
			st.nextToken();  // ignore day
			String t = st.nextToken();

			System.out.println(new Text(ym + ":" + "000".substring(0, 3-t.length()) + t) + "          " + value);
			context.write(new Text(ym + ":" + "000".substring(0, 3-t.length()) + t), value);
		}
	}
	
	public static class WritableMapper extends Mapper<LongWritable, Text, SensorWritable, NullWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) {
			
		}
		
	}
	
	public static class SensorWritable implements Writable, WritableComparable<SensorWritable> {
		
		private String yearMonthString;
		private String sensorValue;

		@Override
		public int compareTo(SensorWritable o) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}

	public static class TempReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();

			for (Text val : values) {
				String s = val.toString();
				System.out.println(s);
				sb.append(s.substring(s.lastIndexOf(",") + 1, s.length()).trim() + " ");
			}

			result.set(sb.toString());

			context.write(new Text(key.toString().substring(0, 7)), result);
		}
	}

	public static class MyPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text text, int numberOfPartitions) {
			return Math.abs(key.toString().substring(0,7).hashCode() % numberOfPartitions);
		}
	}

	public static class MyGroupingComparator extends WritableComparator {
		public MyGroupingComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable wc1, WritableComparable wc2) {
			String s1 = ((Text)wc1).toString();
			String s2 = ((Text)wc2).toString();

			return s1.substring(0,7).compareTo(s2.substring(0, 7));
		}
	}

	public static void main(String[] args) throws Exception {
		
		String inputFile = "resources\\sensordata.txt";
		String outputDir = "output\\examexercises\\second_sort_writable\\"
				+ new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime());
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "seco sort writable");
		FileInputFormat.addInputPath(job, new Path(inputFile));

		job.setJarByClass(SecondarySortingWritable.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(MyPartitioner.class);
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setReducerClass(TempReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.waitForCompletion(true);
		
	}
}