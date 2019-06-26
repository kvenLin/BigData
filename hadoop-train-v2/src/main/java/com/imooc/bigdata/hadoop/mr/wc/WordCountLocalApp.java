package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author louye
 * @title: WordCountApp
 * @description: 使用MR统计HDFS上的文件对应的词频
 * 使用本地文件进行统计, 然后统计结果输出到本地路径
 * @date 2019-06-25,11:35
 */
public class WordCountLocalApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //创建一个job
        Job job = Job.getInstance(configuration);

        //设置Job对应的参数: 主类
        job.setJarByClass(WordCountLocalApp.class);

        //设置Job对应的参数: 自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Job对应的参数: Reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置Job对应的参数: 设置作业输入输出的路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        //提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? 0 : -1);
    }
}
