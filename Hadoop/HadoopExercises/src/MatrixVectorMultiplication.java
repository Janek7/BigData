
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVectorMultiplication {
	
	// MULTPILICATION STEP
	
	private static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString(), ", ");

			if (st.hasMoreTokens()) {				
				String begin = st.nextToken();
				
				if (begin.equalsIgnoreCase("M")) {
					
					context.write(new Text(st.nextToken()+ "-" + st.nextToken()), new IntWritable(Integer.valueOf(st.nextToken())));
					
				} else if (begin.equalsIgnoreCase("V")) {
					
					String colIdx = st.nextToken();
					IntWritable vectorValue = new IntWritable(Integer.valueOf(st.nextToken()));
					int length = Integer.parseInt(context.getConfiguration().get("length"));
					for (int rowIdx = 0; rowIdx < length; rowIdx++) {
						context.write(new Text(String.valueOf(rowIdx) + "-" +  colIdx), vectorValue);
					}
					
				}
				
			}

		}
	
	}
	
	private static class MultiplicationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			Iterator<IntWritable> iterator = values.iterator();
			String keyString = key.toString();
			context.write(new Text(keyString.substring(0, keyString.indexOf("-"))), new IntWritable(iterator.next().get() * iterator.next().get()));
			
		}
		
	}
	
	// ADDITION STEP
	
	private static class AdditionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			String keyString = key.toString();
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
			
		}
		
	}
	
	// MAIN
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[] {"resources\\mvm.txt", "output\\matrix_vector\\mvm" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime()) + "\\multiplied",
				 "output\\matrix_vector\\mvm" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime()) + "\\added"};
		
		// multiplication Job
		Configuration multiplicationConf = new Configuration();
		multiplicationConf.set("length", "3");
		Job multiplicationjob = Job.getInstance(multiplicationConf, "multiplication");
		multiplicationjob.setJarByClass(MatrixVectorMultiplication.class);
		
		multiplicationjob.setMapperClass(TokenizerMapper.class);
		multiplicationjob.setReducerClass(MultiplicationReducer.class);
		// multiplicationjob.setNumReduceTasks(1);
		
		multiplicationjob.setOutputKeyClass(Text.class);
		multiplicationjob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(multiplicationjob, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(multiplicationjob, outputPath);
		multiplicationjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		outputPath.getFileSystem(multiplicationConf).delete(outputPath, true);
		
		multiplicationjob.waitForCompletion(true);
		
		// addition job
		Configuration additionConf = new Configuration();
		additionConf.set("length", "3");
		Job additionjob = Job.getInstance(additionConf, "addition");
		additionjob.setJarByClass(MatrixVectorMultiplication.class);
		
		additionjob.setMapperClass(Mapper.class);
		additionjob.setReducerClass(AdditionReducer.class);
		// additionjob.setNumReduceTasks(1);
		
		additionjob.setOutputKeyClass(Text.class);
		additionjob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(additionjob, outputPath);
		additionjob.setInputFormatClass(SequenceFileInputFormat.class);
		outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(additionjob, outputPath);
		outputPath.getFileSystem(additionConf).delete(outputPath, true);
		
		System.exit(additionjob.waitForCompletion(true) ? 0 : 1);

	}

}
