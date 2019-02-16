package flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortMR {
	
	
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			
			String[] fields = StringUtils.split(line, "\t");
			
			String phoneNB = fields[1];
			long u_flow = Long.parseLong(fields[7]);
			long d_flow = Long.parseLong(fields[8]);
			
			context.write(new FlowBean(phoneNB, u_flow, d_flow), NullWritable.get());
			
		}
		
		
	}
	
	
	
	public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {

			String phoneNB = key.getPhoneNB();
			context.write(new Text(phoneNB), key);
			
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "E:/hadoop-2.4.1");
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SortMR.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path("e:/wc/HTTP_20130313143750.dat"));
		FileOutputFormat.setOutputPath(job, new Path("e:/wc/flowsumoutput1"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
	
	

}
