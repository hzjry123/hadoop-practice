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
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Analyzelog1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Pattern logPattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\] \\\"([^\\\"]*)\\\" ([^ ]*) ([^ ]*).*");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = logPattern.matcher(line);
            if (matcher.matches()){
                String item = matcher.group(5);
                String path = item.split(" ")[1];
                if (path.equals("/assets/img/home-logo.png")){
                    this.word.set(path);
                    context.write(this.word,one);
                }
            }
        }
    }

    public static class IntSumReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for (Iterator i = values.iterator(); i.hasNext(); sum += val.get()) {
                val = (IntWritable) i.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

//        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

//        String[] otherArgs = new String[]{"input/access_log","output"};
//        if (otherArgs.length != 2) {
//            System.err.println("Usage:Merge and duplicate removal <in> <out>");
//            System.exit(2);
//        }

        Job job = Job.getInstance(conf, "analyzelog1");
        job.setJarByClass(Analyzelog1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
