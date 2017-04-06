package ru.zudin.social.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.List;

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

    @Override
    public String getUserId() {
        return Long.toString(userId);
    }

    @Override
    @JsonIgnore
    public List<SocialName> getNames() {
        List<SocialName> strings = Arrays.asList(
                new SocialName("domain", domain, getEntityName()),
                new SocialName("fullName", firstName + " " + lastName, getEntityName()),
                new SocialName("skype", skype, getEntityName()),
                new SocialName("facebook", facebookName, getEntityName()),
                new SocialName("twitter", twitter, getEntityName()),
                new SocialName("instagram", instagram, getEntityName()),
                new SocialName("livejournal", livejournal, getEntityName()),
                new SocialName("nickname", nickname, getEntityName())
        );
        return strings.stream()
                .filter(s -> s.value != null)
                .collect(toList());
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
