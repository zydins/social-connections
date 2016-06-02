package ru.zudin.social.model;

import org.apache.commons.lang3.StringUtils;

import java.net.URL;

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

}
