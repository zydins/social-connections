package ru.zudin.social.mr.social;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sergey
 * @since 12.04.17
 */
public class SocialFactorReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keySplit = key.toString().split(";");
        String userId = keySplit[0];
        String connectionId = keySplit[1];
        Map<String, BigDecimal> factors = new HashMap<>();
        values.forEach(v -> {
            String[] split = v.toString().split(";");
            factors.put(split[0], new BigDecimal(split[1]));
        });
        BigDecimal sum = factors.values().stream()
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal factor = sum.divide(new BigDecimal(factors.size()), 2, BigDecimal.ROUND_UP);
        context.write(new Text(userId), new Text(connectionId + "\t" + factor));
    }
}
