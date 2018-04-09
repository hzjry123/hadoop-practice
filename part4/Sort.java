package com.project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sort {
    public static class TokenizerMapper
            extends Mapper<Object, Text,IntWritable, Text> {
        private Text sort_key = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] temp = value.toString().split("\t");
            System.out.println(temp.length);
            IntWritable count = new IntWritable(Integer.valueOf(temp[1]));
            sort_key.set(temp[0]);
            context.write(count,sort_key);
        }
    }
    public static class IntSumReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key, val);
            }
        }
    }
    private static class myComparator extends IntWritable.Comparator{
        //jing le
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.TokenizerMapper.class);
        job.setCombinerClass(Sort.IntSumReducer.class);
        job.setReducerClass(Sort.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);//change
        job.setOutputValueClass(Text.class);//change
        job.setSortComparatorClass(myComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
