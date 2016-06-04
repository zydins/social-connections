package ru.zudin.social.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import ru.zudin.social.model.SocialUser;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @author sergey
 * @since 04.06.16
 */
public class UserMatchReducer extends Reducer<LongWritable, SocialUser, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<SocialUser> values, Context context) throws IOException, InterruptedException {

    }
}
