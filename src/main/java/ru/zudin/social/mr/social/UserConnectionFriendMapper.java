package ru.zudin.social.mr.social;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.zudin.social.util.UserHelper;

import java.io.IOException;
import java.util.List;

/**
 * @author sergey
 * @since 11.04.17
 */
public class UserConnectionFriendMapper extends Mapper<Text, Text, Text, Text> {

    private UserHelper userHelper;

    public UserConnectionFriendMapper() {
        userHelper = new UserHelper();
    }

    private final static Logger logger = LoggerFactory.getLogger(UserConnectionFriendMapper.class);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if (split.length != 2) {
            logger.warn("Illegal string given: {}", value.toString());
            return;
        }
        String userId = key.toString();
        //if (!userId.equals("vk32030584")) {
        //if (!userId.equals("vk238769438")) {
        //    logger.info("For test purposes only one user is processed, {} is skipped", userId);
        //    return;
        //}
        List<String> friends = userHelper.getFriends(userId);
        if (friends.size() <= 5) {
            return;
        }
        String connectionId = split[0];
        double matchFactor = Double.parseDouble(split[1]);
        for (String friendId : friends) {
            context.write(new Text(friendId), new Text(userId + ";" + connectionId + ";" + matchFactor));
        }
    }

}
