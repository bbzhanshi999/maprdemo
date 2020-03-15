package com.neuedu.serialdemo;


import com.neuedu.serialdemo.bean.FlowWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDemo {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowDemo.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowWritable> {

        private Text phone = new Text();
        private FlowWritable flow = new FlowWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            phone.set(fields[1]);
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);
            long totalFlow = upFlow + downFlow;
            flow.setDownFlow(downFlow);
            flow.setTotalFlow(totalFlow);
            flow.setUpFlow(upFlow);

            context.write(phone, flow);
        }
    }

    public static class FlowReducer extends Reducer<Text, FlowWritable, Text, FlowWritable> {

        private FlowWritable value = new FlowWritable();

        @Override
        protected void reduce(Text key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
            long sum_totalFlow = 0;

            for (FlowWritable fw : values) {
                sum_upFlow += fw.getUpFlow();
                sum_downFlow += fw.getDownFlow();
                sum_totalFlow += fw.getTotalFlow();
            }

            value.setUpFlow(sum_upFlow);
            value.setDownFlow(sum_downFlow);
            value.setTotalFlow(sum_totalFlow);


            context.write(key, value);
        }
    }

}
