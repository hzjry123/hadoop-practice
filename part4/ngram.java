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
import java.util.StringTokenizer;

public class ngram {

//    private static Integer n = 2;
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private static int n;
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String temp;
        public void setup(Context context){
            this.n = context.getConfiguration().getInt("n",2);
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                temp = itr.nextToken();
                if(n <= temp.length()) {
                    for (int i = 0; i <= temp.length() - n; i++) {
                        word.set(temp.substring(i, i + n));
                        context.write(word, one);
                    }
                }
                else {
                    word.set(temp);
                    context.write(word, one);
                }
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
        int n = Integer.valueOf(args[0]);
        Configuration conf = new Configuration();
        conf.set("n",String.valueOf(n));
        //set job
        Job job = Job.getInstance(conf, "com.project1.ngram");
        //set
        job.setJarByClass(ngram.class);
        //Set map output key/value class;
        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}