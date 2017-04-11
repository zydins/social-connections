package ru.zudin.social.mr.social;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author sergey
 * @since 11.04.17
 */
public class UserConnectionFriendMapper extends Mapper<Text, Text, Text, Text> {

    private final static Logger logger = LoggerFactory.getLogger(UserConnectionFriendMapper.class);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if (split.length != 2) {
            logger.warn("Illegal string given: {}", value.toString());
            return;
        }
        String userId = key.toString();
        if (!userId.equals("vk32030584")) {
            logger.info("For test purposes only one user is processed, {} is skipped", userId);
            return;
        }
        List<String> friends = getFriends(userId);
        String connectionId = split[0];
        double matchFactor = Double.parseDouble(split[1]);
        for (String friendId : friends) {
            context.write(new Text(friendId), new Text(userId + ";" + connectionId + ";" + matchFactor));
        }
    }

    private List<String> getFriends(String userId) {
        try {
            Path pt = new Path("input/friends_" + userId + ".txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            List<String> friends = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                String[] split = line.split("\t");
                if (split.length == 2) {
                    String friendId = split[1];
                    friends.add(friendId);
                }
                line = br.readLine();
            }
            return friends;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}
