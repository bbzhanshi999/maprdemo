package com.neuedu.filter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterOutPutFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        FilterRecordWriter filterRecordWriter = new FilterRecordWriter();
        filterRecordWriter.init(job);
        return filterRecordWriter;
    }


    public static class FilterRecordWriter extends RecordWriter<LongWritable,Text>{

        FSDataOutputStream neuedu;
        FSDataOutputStream other;


        public void init(TaskAttemptContext job) throws IOException {
            String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            other = fileSystem.create(new Path(outDir+"/other.log"));
            neuedu = fileSystem.create(new Path(outDir+"/neuedu.log"));
        }


        @Override
        public void write(LongWritable key, Text value) throws IOException, InterruptedException {
            if(value.toString().contains("neuedu"))
                neuedu.write((value.toString()+"\n").getBytes());
            else
                other.write((value.toString()+"\n").getBytes());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(other);
            IOUtils.closeStream(neuedu);
        }
    }
}
