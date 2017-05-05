package ru.zudin.social.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ru.zudin.social.util.StringUtil;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 01.06.16
 */
public class VKUser implements SocialUser {

    public int userId;
    public String firstName;
    public String lastName;
    public String domain;
    public String nickname;

    public String skype;
    public String facebook;
    public String facebookName;
    public String twitter;
    public String instagram;
    public String livejournal;
    @JsonIgnore
    public Set<VKUser> friends = new HashSet<>();

    @Override
    public String getUserId() {
        return Long.toString(userId);
    }

    @Override
    @JsonIgnore
    public List<SocialName> getNames() {
        String fullName = firstName + " " + lastName;
        List<SocialName> strings = Arrays.asList(
                new SocialName("domain", domain, getEntityName()),
                new SocialName("fullName", fullName, getEntityName()),
                new SocialName("skype", skype, getEntityName()),
                new SocialName("facebook", facebookName, getEntityName()),
                new SocialName("twitter", twitter, getEntityName()),
                new SocialName("instagram", instagram, getEntityName()),
                new SocialName("livejournal", livejournal, getEntityName()),
                new SocialName("nickname", nickname, getEntityName())
        );
        Optional<String> optional = StringUtil.transliterate(fullName);
        if (optional.isPresent()) {
            strings.add(new SocialName("fullNameRev", optional.get(), getEntityName()));
        }
        return strings.stream()
                .filter(s -> s.value != null)
                .collect(toList());
    }

    public void addFriends(Collection<VKUser> users) {
        friends.addAll(users);
    }

    @Override
    @JsonIgnore
    public SocialNetwork getEntityName() {
        return SocialNetwork.VK;
    }

    @JsonIgnore
    @Override
    public String getSocialName() {
        return firstName + " " + lastName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VKUser vkUser = (VKUser) o;

        return userId == vkUser.userId;
    }

    @Override
    public int hashCode() {
        return userId;
    }
}
