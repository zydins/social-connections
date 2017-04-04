package ru.zudin.social.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.TwitterUser;
import ru.zudin.social.model.VKUser;

import java.io.IOException;

/**
 * @author sergey
 * @since 06.06.16
 */
public class ParseHelper {

    private final ObjectMapper mapper;

    public ParseHelper() {
        this(new ObjectMapper());
    }

    public ParseHelper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public SocialUser readUser(String value) {
        try {
            String[] split = value.split("\t");
            switch (split[0]) {
                case "VK":
                    return mapper.readValue(split[1], VKUser.class);
                case "Twitter":
                    return mapper.readValue(split[1], TwitterUser.class);
                default:
                    return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
