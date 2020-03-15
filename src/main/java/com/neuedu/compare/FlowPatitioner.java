package com.neuedu.compare;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPatitioner extends Partitioner<FlowWritable,Text > {


    /**
     * 根据手机号前缀来划分分区
     * @param text
     * @param flowWritable
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(FlowWritable flowWritable,Text text,int numPartitions) {

        String phone = text.toString().substring(0,3);

        switch (phone){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;

        }


    }
}
