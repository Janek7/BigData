
import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVectorMultiplicationWritable {
	
	private static class PositionWritable  implements Writable, WritableComparable<PositionWritable> {
		
		private int row;
		private int col;
		
		public PositionWritable() {}
				
		public PositionWritable(int row, int col) {
			this.row = row;
			this.col = col;
		}
		
		public PositionWritable(String row, String col) {
			this(Integer.valueOf(row), Integer.valueOf(col));
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			row = in.readInt();
			col = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(row);
			out.writeInt(col);
		}
		
		@Override
		public int compareTo(PositionWritable positionIterable) {
			return this.toString().compareTo(positionIterable.toString());
		}
		
		@Override
		public String toString() {
			return String.valueOf(row) + String.valueOf(col);
		}
		
		public int getRow() {
			return row;
		}
		
		public void setRow(int row) {
			this.row = row;
		}
		
		public int getCol() {
			return col;
		}
		
		public void setCol(int col) {
			this.col = col;
		}
		
	}
	
	// MULTPILICATION STEP
	
	private static class TokenizerMapper extends Mapper<LongWritable, Text, PositionWritable, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString(), ", ");

			if (st.hasMoreTokens()) {				
				String begin = st.nextToken();
				
				if (begin.equalsIgnoreCase("M")) {
					
					context.write(new PositionWritable(st.nextToken(), st.nextToken()), new IntWritable(Integer.valueOf(st.nextToken())));
					
				} else if (begin.equalsIgnoreCase("V")) {
					
					String colIdx = st.nextToken();
					IntWritable vectorValue = new IntWritable(Integer.valueOf(st.nextToken()));
					int length = Integer.parseInt(context.getConfiguration().get("length"));
					for (int rowIdx = 0; rowIdx < length; rowIdx++) {
						context.write(new PositionWritable(String.valueOf(rowIdx),  colIdx), vectorValue);
					}
					
				}
				
			}

		}
	
	}
	
	private static class MultiplicationReducer extends Reducer<PositionWritable, IntWritable, PositionWritable, IntWritable> {
		
		private static final int COL_VALUE = 0;
		
		@Override
		public void reduce(PositionWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			Iterator<IntWritable> iterator = values.iterator();
			context.write(new PositionWritable(key.getRow(), COL_VALUE), new IntWritable(iterator.next().get() * iterator.next().get()));
			
		}
		
	}
	
	// ADDITION STEP
	
	private static class AdditionReducer extends Reducer<PositionWritable, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(PositionWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(new Text(String.valueOf(key.getRow())), new IntWritable(sum));
			
		}
		
	}
	
	// MAIN
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[] {"resources\\mvm.txt", "output\\matrix_vector_writable\\mvm_w" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime()) + "\\multiplied",
				 "output\\matrix_vector_writable\\mvm_w" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss", Locale.GERMANY).format(Calendar.getInstance().getTime()) + "\\added"};
		
		// multiplication Job
		Configuration multiplicationConf = new Configuration();
		multiplicationConf.set("length", "3");
		Job multiplicationJob = Job.getInstance(multiplicationConf, "multiplication");
		multiplicationJob.setJarByClass(MatrixVectorMultiplicationWritable.class);
		
		multiplicationJob.setMapperClass(TokenizerMapper.class);
		multiplicationJob.setReducerClass(MultiplicationReducer.class);
		// multiplicationjob.setNumReduceTasks(1);
		
		multiplicationJob.setOutputKeyClass(PositionWritable.class);
		multiplicationJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(multiplicationJob, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(multiplicationJob, outputPath);
		multiplicationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		outputPath.getFileSystem(multiplicationConf).delete(outputPath, true);
		
		multiplicationJob.waitForCompletion(true);
		
		// addition job
		Configuration additionConf = new Configuration();
		additionConf.set("length", "3");
		Job additionJob = Job.getInstance(additionConf, "addition");
		additionJob.setJarByClass(MatrixVectorMultiplicationWritable.class);
		
		additionJob.setMapperClass(Mapper.class);
		additionJob.setReducerClass(AdditionReducer.class);
		// additionjob.setNumReduceTasks(1);
		additionJob.setOutputKeyClass(PositionWritable.class);
		additionJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(additionJob, outputPath);
		additionJob.setInputFormatClass(SequenceFileInputFormat.class);
		outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(additionJob, outputPath);
		outputPath.getFileSystem(additionConf).delete(outputPath, true);
		
		System.exit(additionJob.waitForCompletion(true) ? 0 : 1);

	}

}
