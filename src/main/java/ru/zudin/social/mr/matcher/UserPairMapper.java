package ru.zudin.social.mr.matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sergey
 * @since 06.04.17
 */
public class UserPairMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String firstUser = split[0];
        String secondUser = split[1];
        context.write(new Text(firstUser), value);
        context.write(new Text(secondUser), value);
    }
}
