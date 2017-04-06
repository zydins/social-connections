package ru.zudin.social.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public interface SocialUser {

    @JsonIgnore
    default String getGlobalId() {
        SocialNetwork entityName = getEntityName();
        int end = Math.min(3, entityName.name().length());
        String prefix = entityName.name().substring(0, end).toLowerCase();
        return prefix + getUserId();
    }

    String getUserId();

    List<SocialName> getNames();

    SocialNetwork getEntityName();

    String getSocialName();

}
