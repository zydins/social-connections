package ru.zudin.social.mr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.parse.SocialParser;
import ru.zudin.social.parse.TwitterParser;
import ru.zudin.social.parse.VKParser;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * @author sergey
 * @since 04.06.16
 */
public class EntityMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String s = value.toString();
            SocialParser parser = null;
            long userId = 0;
            int depth = 0;
            if (s.startsWith("VK")) {
                String[] values = s.split(":");
                userId = Long.parseLong(values[1]);
                parser = new VKParser();
                depth = Integer.parseInt(values[2]);
            } else if (s.startsWith("Twitter")) {
                String[] values = s.split(":");
                String userName = values[1];
                TwitterParser twitterParser = new TwitterParser();
                parser = twitterParser;
                Optional<Long> id = twitterParser.getId(userName);
                if (!id.isPresent()) {
                    logger.warn("Found invalid user '{}'", userName);
                    return;
                }
                userId = id.get();
                depth = Integer.parseInt(values[2]);
            }
            if (parser == null || depth < 0 || userId <= 0) {
                logger.error("Invalid input string");
                return;
            }
            List<? extends SocialUser> users = parser.parse(userId, depth);
            ObjectMapper mapper = new ObjectMapper();
            for (SocialUser user : users) {
                context.write(new Text(user.getEntityName().name()), new Text(mapper.writeValueAsString(user)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
