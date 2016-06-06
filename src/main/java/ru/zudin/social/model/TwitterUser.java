package ru.zudin.social.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 02.06.16
 */
public class TwitterUser implements SocialUser {

    public long userId;
    public String name;
    public String nickname;
    public String info;
    public String url;
    public String otherNickname;

    public static void parseOtherNickname(TwitterUser user) {
        if (StringUtils.isBlank(user.url)) {
            return;
        }
        try {
            URL url = new URL(user.url);
            user.otherNickname = url.getPath().replace("/", "");
        } catch (Exception e) {
            return;
        }
    }

    @Override
    public long getUserId() {
        return userId;
    }

    @Override
    @JsonIgnore
    public List<String> getNames() {
        List<String> strings = Arrays.asList(nickname, otherNickname, name, info);
        return strings.stream()
                .filter(s -> s != null)
                .distinct()
                .collect(toList());
    }

    @Override
    @JsonIgnore
    public String getEntityName() {
        return "Twitter";
    }

    @Override
    public String getSocialName() {
        return nickname;
    }

}
