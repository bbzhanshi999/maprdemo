package com.neuedu.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RJDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());


        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(RJGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("f:/input"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

        private OrderBean k = new OrderBean();
        private String fileName;

        /**
         * 获取切片的文件原文件名，根据文件名来采取不同输出数据策略
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (fileName.contains("order")) {
                k.setOid(split[0]);
                k.setPid(split[1]);
                k.setAmount(Integer.valueOf(split[2]));
                k.setPname("");
            } else {
                k.setOid("");
                k.setPid(split[0]);
                k.setAmount(0);
                k.setPname(split[1]);
            }
            context.write(k, NullWritable.get());
        }
    }


    public static class RJGroupingComparator extends WritableComparator {

        public RJGroupingComparator() {
            super(OrderBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            OrderBean oa = (OrderBean) a;
            OrderBean ob = (OrderBean) b;


            return oa.getPid().compareTo(ob.getPid());
        }
    }


    public static class RJReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //拿到的第一个key就是商品表的数据，将商品pname设置给后续的key，然后写出
            values.iterator().next();
            String pname = key.getPname();
            while (values.iterator().hasNext()) {
                values.iterator().next();
                key.setPname(pname);
                context.write(key, NullWritable.get());
            }
        }
    }


    public static class OrderBean implements WritableComparable<OrderBean> {

        private String pid;
        private String oid;
        private Integer amount;

        public String getPid() {
            return pid;
        }

        public void setPid(String pid) {
            this.pid = pid;
        }

        public String getOid() {
            return oid;
        }

        public void setOid(String oid) {
            this.oid = oid;
        }

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }

        public String getPname() {
            return pname;
        }

        public void setPname(String pname) {
            this.pname = pname;
        }

        private String pname;


        @Override
        public int compareTo(OrderBean o) {
            int compare = pid.compareTo(o.getPid());

            if (compare == 0) {
                compare = o.getPname().compareTo(this.getPname());
                if (compare == 0) {
                    compare = oid.compareTo(o.getOid());
                }
            }
            return compare;

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(oid);
            dataOutput.writeUTF(pid);
            dataOutput.writeUTF(pname);
            dataOutput.writeInt(amount);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            oid = dataInput.readUTF();
            pid = dataInput.readUTF();
            pname = dataInput.readUTF();
            amount = dataInput.readInt();
        }

        @Override
        public String toString() {
            return oid + "\t" + pname + "\t" + amount;
        }
    }

}
