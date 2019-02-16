package areapartition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private String phoneNB;
    private long up_flow;
    private long d_flow;
    private long s_flow;

    //反序列化时需要空参构造函数
    public FlowBean() {
    }

    public FlowBean(String phoneNB, long up_flow, long d_flow) {
        this.phoneNB = phoneNB;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        s_flow = up_flow + d_flow;
    }

    /**
     * 将对象数据序列化到流中
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNB);
        dataOutput.writeLong(up_flow);
        dataOutput.writeLong(d_flow);
        dataOutput.writeLong(s_flow);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        //从数据流中读取对象字段时，必须跟序列化时保持一致
        phoneNB = dataInput.readUTF();
        up_flow = dataInput.readLong();
        d_flow = dataInput.readLong();
        s_flow = dataInput.readLong();
    }

    @Override
    public String toString() {
       return d_flow + "\t" + up_flow + "\t" + s_flow;
    }

    public String getPhoneNB() {
        return phoneNB;
    }

    public void setPhoneNB(String phoneNB) {
        this.phoneNB = phoneNB;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }
}
