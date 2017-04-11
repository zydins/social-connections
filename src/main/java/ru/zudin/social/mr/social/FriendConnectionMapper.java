package ru.zudin.social.mr.social;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sergey
 * @since 11.04.17
 */
public class FriendConnectionMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(11);
        super.map(key, value, context);
    }
}
