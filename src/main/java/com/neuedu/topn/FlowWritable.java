package com.neuedu.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowWritable implements WritableComparable<FlowWritable> {

    private Long upFlow;
    private Long downFlow;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(Long totalFlow) {
        this.totalFlow = totalFlow;
    }

    private Long totalFlow;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(totalFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        totalFlow = dataInput.readLong();
    }
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    @Override
    public int compareTo(FlowWritable o) {
        return Long.compare(o.getTotalFlow(),this.getTotalFlow());
    }
}
