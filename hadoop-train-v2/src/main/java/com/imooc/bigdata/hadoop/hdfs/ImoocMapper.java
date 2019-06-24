package com.imooc.bigdata.hadoop.hdfs;

/**
 * @author louye
 * @title: ImoocMapper
 * @description: 自定义Mapper
 * @date 2019-06-24,20:56
 */
public interface ImoocMapper {
    /**
     * @param line 读取到的每一行数据
     * @param context 上下文/缓存
     */
    void map(String line, ImoocContext context);
}
