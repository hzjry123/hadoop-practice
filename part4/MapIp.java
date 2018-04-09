package com.project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapIp {
    public static class TokenizerMapper
        extends Mapper<Object, Text,Text, IntWritable> {
//        private String regex = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
        String regex = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
        Pattern pattern = Pattern.compile(regex);
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Matcher m = pattern.matcher(line);
            while(m.find()) {

                word.set(m.group());
                context.write(word, one);
            }
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
//        String regex = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
//        Pattern pattern = Pattern.compile(regex);
//        String line = "10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] \"GET /assets/img/home-logo.png HTTP/1.1\" 200 10469";
//        Matcher m = pattern.matcher(line);
//        while(m.find()) {
//            System.out.println(m.group());
//        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "log ip analyze");
        job.setJarByClass(MapIp.class);
        job.setMapperClass(MapIp.TokenizerMapper.class);
        job.setCombinerClass(MapIp.IntSumReducer.class);
        job.setReducerClass(MapIp.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
