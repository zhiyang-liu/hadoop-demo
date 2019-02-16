package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

public class Demo1 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        /**
         * hdfs上传下载文件
         */
        /*Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "hadoop");
        fs.copyFromLocalFile(new Path("d:/liuzhiyang.txt"), new Path("hdfs://ns1/"));*/

        //如果报当前用户没有权限，可以在VM options中添加   -DHADOOP_USER_NAME=hadoop
        /**
         * Wrong FS: hdfs://ns1/test.txt, expected: file:///
         * 上面的错误是没有加载配置文件，原因是只有第一次复制配置文件好使
         * 如果上面的错误，可以把配置文件复制到resourcesxia，或复制到java下再执行，复制一次执行一次
         */
        Configuration conf = new Configuration();
        Path src = new Path("hdfs://ns1/test.txt");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(src);
        FileOutputStream out = new FileOutputStream("e:/test.txt");
        IOUtils.copy(in, out);
    }
}
