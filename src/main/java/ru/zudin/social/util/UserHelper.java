package ru.zudin.social.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author sergey
 * @since 12.04.17
 */
public class UserHelper {

    public Map<String, BigDecimal> getConnection(String userId) {
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

    public List<String> getFriends(String userId) {
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
