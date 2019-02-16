package inverseindex;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InverseIndexStepOne {

	/**
	 * 第一步
	 * hello-->a.txt	2
	 * hello-->b.txt	1
	 * hello-->c.txt	1
	 * nihao-->a.txt	1
	 * nihao-->b.txt	1
	 */
	public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {


			String line = value.toString();

			String[] fields = StringUtils.split(line, " ");
			

			FileSplit inputSplit = (FileSplit) context.getInputSplit();

			String fileName = inputSplit.getPath().getName();
			
			for(String field:fields){

				context.write(new Text(field+"-->"+fileName), new LongWritable(1));
				
			}
			
		}
		
		
	}
	
	
	public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {

			long counter = 0;
			for(LongWritable value:values){
				
				counter += value.get();
				
			}
			
			context.write(key, new LongWritable(counter));
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "E:/hadoop-2.4.1");
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InverseIndexStepOne.class);
		
		job.setMapperClass(StepOneMapper.class);
		job.setReducerClass(StepOneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:/wc/inverseindex/"));

		Path output = new Path("E:/wc/inverseindexoutput1/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}

}
