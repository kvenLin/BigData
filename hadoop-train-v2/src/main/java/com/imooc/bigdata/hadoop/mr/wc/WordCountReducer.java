package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author louye
 * @title: WordCountReducer
 * @description: TODO
 * @date 2019-06-25,11:22
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * (hello, 1) (world, 1)
     * (hello, 1) (world, 1)
     * (hello, 1) (world, 1)
     * (welcome, 1)
     *
     * map输出到reduce端, 是按照相同的key分发到一个reduce上去执行
     *
     * reduce1: (hello, 1) (hello, 1) (hello, 1) ===> (hello, <1,1,1>)
     * reduce2: (world, 1) (world, 1) (world, 1) ===> (world, <1,1,1>)
     * reduce3: (welcome, 1) ===> (welcome, <1>)
     *
     * Reducer和Mapper中其实使用到了模板设计模式
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
