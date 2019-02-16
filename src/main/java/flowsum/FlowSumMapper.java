package flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = StringUtils.split(line, '\t');
        String phoneNB = fields[1];
        long up_flow = Long.parseLong(fields[7]);
        long d_flow = Long.parseLong(fields[8]);
        context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, d_flow));
    }
}
