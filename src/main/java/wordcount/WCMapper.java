package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;

//四个泛型，前两个为mapper输入数据的类型，默认框架传入mappper的输入类型为文本行起始偏移量，文本内容.
//JDK的类型序列化会有冗余信息
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //mapreduce每读一行就会调用一次该方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, ' ');
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
