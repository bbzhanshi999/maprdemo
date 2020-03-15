package com.neuedu.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderDriver {

    
     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
             Job job = Job.getInstance(new Configuration());
     
             job.setMapperClass(OrderMapper.class);
             job.setReducerClass(OrderReducer.class);
     
             job.setMapOutputKeyClass(OrderWritable.class);
             job.setMapOutputValueClass(NullWritable.class);
     
             job.setOutputKeyClass(OrderWritable.class);
             job.setOutputValueClass(NullWritable.class);

             job.setGroupingComparatorClass(OrderGroupingComparator.class);
     
             FileInputFormat.setInputPaths(job, new Path("f:/input"));
             FileOutputFormat.setOutputPath(job, new Path("f:/output"));
     
             boolean result = job.waitForCompletion(true);
             System.exit(result ? 0 : 1);
         }






    public static class OrderMapper extends Mapper<LongWritable, Text,OrderWritable, NullWritable/*NullWritable就是hadoop对于空值的序列化*/>{

        private OrderWritable k  = new OrderWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");

            k.setOrderId(line[0]);
            k.setProductId(line[1]);
            k.setPrice(Double.parseDouble(line[2]));

            context.write(k,NullWritable.get());
        }
    }


    public static class OrderReducer extends Reducer<OrderWritable,NullWritable,OrderWritable,NullWritable>{
        @Override
        protected void reduce(OrderWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            while(values.iterator().hasNext()){
                context.write(key,values.iterator().next());
            }
        }
    }


    public static class OrderGroupingComparator extends WritableComparator{


        public OrderGroupingComparator() {
            super(OrderWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            //比较orderid是否想同
            OrderWritable oa = (OrderWritable) a;
            OrderWritable ob = (OrderWritable) b;
            return oa.getOrderId().compareTo(ob.getOrderId());
        }
    }




    public static class OrderWritable implements WritableComparable<OrderWritable> {

        private String orderId;

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public Double getPrice() {
            return price;
        }

        public void setPrice(Double price) {
            this.price = price;
        }

        private String productId;
        private Double price;


        @Override
        public int compareTo(OrderWritable o) {

            int compareOrderId = this.orderId.compareTo(o.getOrderId());

            if(compareOrderId==0){
                return Double.compare(o.getPrice(),this.getPrice());
            }

            return compareOrderId;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(orderId);
            dataOutput.writeUTF(productId);
            dataOutput.writeDouble(price);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            orderId = dataInput.readUTF();
            productId = dataInput.readUTF();
            price = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return orderId+"\t"+productId+"\t"+price;
        }
    }


}
