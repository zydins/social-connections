package ru.zudin.social.mr.social;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sergey
 * @since 11.04.17
 */
public class FriendConnectionMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String friendId = key.toString();
        String[] split = value.toString().trim().split(";");
        String connectionId = split[1];
        String userId = split[0];
        BigDecimal matchFactor = new BigDecimal(split[2]);
        Map<String, BigDecimal> connectionIds = getConnection(friendId);
        for (String friendConnId : connectionIds.keySet()) {
            context.write(new Text(friendConnId), new Text(userId + ";" + connectionId + ";" +
                    friendId + ";" + matchFactor + ";" + connectionIds.get(friendConnId)));
        }
    }

    private Map<String, BigDecimal> getConnection(String userId) {
        try {
            Path pt = new Path("match/" + userId);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            Map<String, BigDecimal> friends = new HashMap<>();
            String line = br.readLine();
            while (line != null) {
                String[] split = line.split("\t");
                if (split.length == 3) {
                    String friendId = split[1];
                    BigDecimal factor = new BigDecimal(split[2]);
                    friends.put(friendId, factor);
                }
                line = br.readLine();
            }
            return friends;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

}
