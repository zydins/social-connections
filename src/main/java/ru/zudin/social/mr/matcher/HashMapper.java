package ru.zudin.social.mr.matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.util.HashUtils;
import ru.zudin.social.util.ParseHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 04.06.16
 */
public class HashMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final ParseHelper parseHelper;

    public HashMapper() {
        this.parseHelper = new ParseHelper();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        SocialUser user = parseHelper.readUser(string);
        if (user == null) {
            return;
        }
        List<Integer> hashes = user.getNames().stream()
                .map(n -> n.value)
                .map(HashUtils::shinglingHash)
                .flatMap(Collection::stream)
                .collect(toList());
        for (Integer hash : hashes) {
            context.write(new LongWritable(hash), new Text(string));
        }
    }
}
