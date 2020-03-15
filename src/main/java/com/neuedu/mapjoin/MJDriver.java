package com.neuedu.mapjoin;

import com.neuedu.reducejoin.RJDriver;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MJDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MJDriver.class);

        job.setMapperClass(MJMapper.class);


        job.setMapOutputKeyClass(RJDriver.OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///f:/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("f:/input/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output"));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    public static class MJMapper extends Mapper<LongWritable, Text, RJDriver.OrderBean, NullWritable> {

        Map<String,String> cache = new HashMap<>();
        RJDriver.OrderBean k = new RJDriver.OrderBean();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String path = context.getCacheFiles()[0].getPath().toString();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path)), "UTF-8"));
            String line;
            while(StringUtils.isNotEmpty(line = br.readLine())){
                String[] split = line.split("\t");
                cache.put(split[0],split[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            k.setPname(cache.get(split[1]));
            k.setPid(split[1]);
            k.setOid(split[0]);
            k.setAmount(Integer.valueOf(split[2]));
            context.write(k,NullWritable.get());
        }
    }

}
