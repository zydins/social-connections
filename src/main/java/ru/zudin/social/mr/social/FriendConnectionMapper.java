package ru.zudin.social.mr.social;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.social.util.UserHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * @author sergey
 * @since 11.04.17
 */
public class FriendConnectionMapper extends Mapper<Text, Text, Text, Text> {

    private UserHelper userHelper;

    public FriendConnectionMapper() {
        userHelper = new UserHelper();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String friendId = key.toString();
        String[] split = value.toString().trim().split(";");
        String connectionId = split[1];
        String userId = split[0];
        BigDecimal matchFactor = new BigDecimal(split[2]);
        Map<String, BigDecimal> connectionIds = userHelper.getConnection(friendId);
        for (String friendConnId : connectionIds.keySet()) {
            context.write(new Text(connectionId), new Text(userId + ";" + friendId + ";" + friendConnId + ";"
                    + matchFactor + ";" + connectionIds.get(friendConnId)));
        }
    }



}
