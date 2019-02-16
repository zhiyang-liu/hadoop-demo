package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCRunner {
   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       /**
        * 本地与在本地提交集群都需要下句,但是如果打jar包上传到集群运行需要注释掉
        * 如果报当前用户没有权限，可以在VM options中添加   -DHADOOP_USER_NAME=hadoop
        */
       System.setProperty("hadoop.home.dir", "E:/hadoop-2.4.1");
       Configuration conf = new Configuration();

       /**
        * 本地提交集群运行，需要以下配置；
        * 想再本地运行注释掉即可
        */
       //conf.set("mapreduce.framework.name", "yarn");
       //conf.set("mapreduce.app-submission.cross-platform", "true");
       //conf.set("mapreduce.job.jar","target/hadoopexcise-1.0-SNAPSHOT.jar");//不需要每次运行都重新打jar包，只需要制定就可以了

       Job job = Job.getInstance(conf);

       //设置整个job所用的类在哪些jar包
       job.setJarByClass(WCRunner.class);

       //设置job使用的mapper和reducer类
       job.setMapperClass(WCMapper.class);
       job.setReducerClass(WCReducer.class);

       //设置reduce的输出类型
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(LongWritable.class);

       //设置map的输出类型
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(LongWritable.class);

       //指定原始数据存放的路径
       FileInputFormat.setInputPaths(job, new Path("e:/wc/srcdata/"));
       FileOutputFormat.setOutputPath(job, new Path("e:/wc/output7/"));

       //将job提交给集群运行
       job.waitForCompletion(true);
   }
}
