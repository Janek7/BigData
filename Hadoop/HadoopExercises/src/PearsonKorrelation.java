import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// http://www.crashkurs-statistik.de/der-korrelationskoeffizient-nach-pearson/

public class PearsonKorrelation {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable dw = new DoubleWritable();
		private Text[] elem = {new Text("xvg"), new Text("yvg")};

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			itr.nextToken(); // skip ID

			for (int i = 0; i < 2; i++) {
				dw.set(Double.parseDouble(itr.nextToken()));
				context.write(elem[i], dw);
			}
		}
	}
	
	public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			int cnt = 0;

			for (DoubleWritable val : values) {
				sum += val.get();
				cnt++;
			}
			
			result.set(sum/cnt);
			context.write(key, result);
		}
	}

	public static class SquareMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable dw = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			int xsq = 0;
			int ysq = 0;
			int sum = 0;
			int cnt = 1;

			itr.nextToken(); // skip id

			int x = Integer.parseInt(itr.nextToken());
			int y = Integer.parseInt(itr.nextToken());
			xsq += x*x;
			ysq += y*y;
			sum += x*y;

			dw.set(xsq);
			context.write(new Text("xsq"), dw);
			dw.set(ysq);
			context.write(new Text("ysq"), dw);
			dw.set(sum);
			context.write(new Text("sum"), dw);
			dw.set(cnt);
			context.write(new Text("cnt"), dw);
		}
	}


	public static class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class FinalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private Map<String, Double> map = new HashMap<>();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			for (DoubleWritable val : values) {
				map.put(key.toString(), val.get());
//				System.out.println(key.toString());
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			double cnt = map.get("cnt");
			double r = (map.get("sum") - (cnt * map.get("xvg") * map.get("yvg"))) /
					   (Math.sqrt(map.get("xsq")-cnt *map.get("xvg")*map.get("xvg")) * Math.sqrt(map.get("ysq")-cnt*map.get("yvg")*map.get("yvg")));
			result.set(r);

			context.write(new Text("correlation"), result);
			System.out.println("--------------------------");
		}
	}

	public static class MyPartitioner extends Partitioner<Text, DoubleWritable> {
		public int getPartition(Text k, DoubleWritable value, int numPartitions) { 
			return 0;
		}
	}

	public static void main(String[] args) throws Exception {
		
		String dateString = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime());
		args = new String[] {"resources\\corr.txt", 
				"output\\pearson_korrelation\\pkor" + dateString + "\\avg", 
				"output\\pearson_korrelation\\pkor" + dateString + "\\sq", 
				"output\\pearson_korrelation\\pkor" + dateString + "\\sumRes", 
				"output\\pearson_korrelation\\pkor" + dateString + "\\corr", };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "average");

		job.setJarByClass(PearsonKorrelation.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);

		// -----------------------

		conf = new Configuration();
		job = Job.getInstance(conf, "squaring");

		job.setJarByClass(PearsonKorrelation.class);
		job.setMapperClass(SquareMapper.class);
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);

		// -----------------------

		conf = new Configuration();
		job = Job.getInstance(conf, "sumResults");

		job.setJarByClass(PearsonKorrelation.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(DoubleSumReducer.class);

		MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, Mapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		outputPath = new Path(args[3]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);

		// -----------------------

		conf = new Configuration();
		job = Job.getInstance(conf, "finalize");
		job.setNumReduceTasks(1);

		job.setJarByClass(PearsonKorrelation.class);
		job.setReducerClass(FinalReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[3]));
		outputPath = new Path(args[4]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);		
		System.exit(0);
	}

}