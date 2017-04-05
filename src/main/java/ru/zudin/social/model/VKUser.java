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
    public List<String> getNames() {
        List<String> strings = Arrays.asList(livejournal, instagram, twitter, facebookName, skype,
                domain, firstName + " " + lastName, nickname);
        return strings.stream()
                .filter(s -> s != null)
                .distinct()
                .collect(toList());
    }

    @Override
    @JsonIgnore
    public String getEntityName() {
        return "VK";
    }

    @JsonIgnore
    @Override
    public String getSocialName() {
        return firstName + " " + lastName;
    }

}
