package ru.zudin.social.mr.social;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * @author sergey
 * @since 12.04.17
 */
public class MaxFactorReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String[] split = key.toString().split(";");
        String userId = split[0];
        String friendId = split[1];
        String connectionId = split[2];
        Optional<Double> optional = StreamSupport.stream(values.spliterator(), false)
                .map(DoubleWritable::get)
                .max(Double::compare);
        if (optional.isPresent()) {
            Double max = optional.get();
            context.write(new Text(userId + ";" + connectionId), new Text(friendId + ";" + max));
        }
    }
}
