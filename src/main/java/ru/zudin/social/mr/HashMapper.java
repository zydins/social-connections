package ru.zudin.social.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.util.HashUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 04.06.16
 */
public class HashMapper extends Mapper<Text, SocialUser, LongWritable, SocialUser> {

    @Override
    protected void map(Text key, SocialUser value, Context context) throws IOException, InterruptedException {
        List<String> names = value.getNames();
        List<Integer> hashes = names.stream().map(HashUtils::shinglingHash).flatMap(Collection::stream).collect(toList());
        for (Integer hash : hashes) {
            context.write(new LongWritable(hash), value);
        }
    }
}
