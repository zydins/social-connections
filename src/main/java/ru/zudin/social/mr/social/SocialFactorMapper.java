package ru.zudin.social.mr.social;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author sergey
 * @since 11.04.17
 */
public class SocialFactorMapper extends Mapper<Text, Text, Text, DoubleWritable> {

    private final static Logger logger = LoggerFactory.getLogger(SocialFactorMapper.class);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
