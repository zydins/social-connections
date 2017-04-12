package ru.zudin.social.mr.social;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.zudin.social.util.UserHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author sergey
 * @since 11.04.17
 */
public class FriendMatchFactorMapper extends Mapper<Text, Text, Text, DoubleWritable> {

    private final static Logger logger = LoggerFactory.getLogger(FriendMatchFactorMapper.class);

    private UserHelper userHelper;

    public FriendMatchFactorMapper() {
        userHelper = new UserHelper();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String connectionId = key.toString();
        List<String> connectionFriends = userHelper.getFriends(connectionId);

        String[] split = value.toString().split(";");
        String userId = split[0];
        String friendId = split[1];
        String friendConnId = split[2];
        BigDecimal matchFactor = new BigDecimal(split[3]);
        BigDecimal friendMatchFactor = new BigDecimal(split[4]);

        double factor = 0.0;
        if (connectionFriends.contains(friendConnId)) { //is friend
            factor = friendMatchFactor.doubleValue();
        }
        context.write(new Text(userId + ";" + friendId + ";" + connectionId),
                new DoubleWritable(factor));

    }
}
