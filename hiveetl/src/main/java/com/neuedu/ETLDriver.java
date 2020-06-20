package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }


    public static class ETLMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

        private final Text k=  new Text();
        StringBuilder sb = new StringBuilder();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String newData = etlData(value.toString());
            if(newData==null){
                context.getCounter("etl","fail").increment(1);
            }else{
                context.getCounter("etl","success").increment(1);
                k.set(newData);
                context.write(k,NullWritable.get());
            }
        }

        private String etlData(String value) {
            String[] fields = value.split("\t");
            if(fields.length<9){
                return null;
            }
            fields[3] = fields[3].replace(" ","");
            sb.delete(0,sb.length());
            for(int i = 0;i<fields.length;i++){
                if(i==fields.length-1){
                    sb.append(fields[i]);
                }else if(i<9){
                    sb.append(fields[i]).append("\t");
                }else{
                    sb.append(fields[i]).append("&");
                }
            }

            return sb.toString();
        }
    }
}
