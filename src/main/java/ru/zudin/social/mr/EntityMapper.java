package ru.zudin.social.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.parse.SocialParser;
import ru.zudin.social.parse.TwitterParser;
import ru.zudin.social.parse.VKParser;
import twitter4j.TwitterException;

import java.io.IOException;
import java.util.List;

/**
 * @author sergey
 * @since 04.06.16
 */
public class EntityMapper extends Mapper<LongWritable, Text, Text, SocialUser> {

    private final static Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] split = value.toString().split("\t");
            SocialParser parser = null;
            long userId = 0;
            int depth = 0;
            for (String s : split) {
                if (s.startsWith("VK")) {
                    String[] values = s.split(":");
                    userId = Long.parseLong(values[1]);
                    parser = new VKParser();
                } else if (s.startsWith("Twitter")) {
                    String[] values = s.split(":");
                    String userName = values[1];
                    TwitterParser twitterParser = new TwitterParser();
                    parser = twitterParser;
                    userId = twitterParser.getId(userName);
                } else if (s.startsWith("Depth")) {
                    String[] values = s.split(":");
                    depth = Integer.parseInt(values[1]);
                }
            }
            if (parser == null || depth < 0 || userId <= 0) {
                logger.error("Invalid input string");
                return;
            }
            List<? extends SocialUser> users = parser.parse(userId, depth);
            for (SocialUser user : users) {
                context.write(new Text(user.getEntityName()), user);
            }
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }
}
