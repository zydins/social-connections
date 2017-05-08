package ru.zudin.social.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import ru.zudin.social.util.StringUtil;

import java.util.*;

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
    @JsonIgnore
    public Set<TwitterUser> friends = new HashSet<>();

    public static void parseOtherNickname(TwitterUser user) {
        if (StringUtils.isBlank(user.url)) {
            return;
        }
        try {
            String[] split = user.url.split("/");
            user.otherNickname = split[split.length - 1]; //TODO: add host
        } catch (Exception ignored) {

        }
    }

    @Override
    public String getUserId() {
        return Long.toString(userId);
    }

    public void addFriends(Collection<TwitterUser> users) {
        friends.addAll(users);
    }

    @Override
    @JsonIgnore
    public List<SocialName> getNames() {
        List<SocialName> strings = new ArrayList<>(Arrays.asList(
                new SocialName("nickname", nickname, getEntityName()),
                new SocialName("otherNickname", otherNickname, getEntityName()),
                new SocialName("name", name, getEntityName()),
                new SocialName("info", info, getEntityName())
        ));
        Optional<String> optional = StringUtil.transliterate(name);
        if (optional.isPresent()) {
            strings.add(new SocialName("nameRev", optional.get(), getEntityName()));
        }
        return strings.stream()
                .filter(s -> s.value != null)
                .collect(toList());
    }

    @Override
    @JsonIgnore
    public SocialNetwork getEntityName() {
        return SocialNetwork.Twitter;
    }

    @JsonIgnore
    @Override
    public String getSocialName() {
        return nickname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TwitterUser that = (TwitterUser) o;

        return userId == that.userId;
    }

    @Override
    public int hashCode() {
        return (int) (userId ^ (userId >>> 32));
    }
}
