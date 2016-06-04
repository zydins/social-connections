package ru.zudin.social.model;

import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static ru.zudin.social.util.EntityUtils.readProperty;
import static ru.zudin.social.util.EntityUtils.writeProperty;

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
    public long getId() {
        return userId;
    }

    @Override
    public List<String> getNames() {
        List<String> strings = Arrays.asList(nickname, otherNickname, name, info);
        return strings.stream()
                .distinct()
                .collect(toList());
    }

    @Override
    public String getEntityName() {
        return "Twitter";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(userId);
        writeProperty(out, name);
        writeProperty(out, nickname);
        writeProperty(out, info);
        writeProperty(out, url);
        writeProperty(out, otherNickname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            userId = in.readLong();
            readProperty(in, "name", this);
            readProperty(in, "nickname", this);
            readProperty(in, "info", this);
            readProperty(in, "url", this);
            readProperty(in, "otherNickname", this);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }
}
