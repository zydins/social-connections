package ru.zudin.social.model;

import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public interface SocialUser {

    default String getGlobalId() {
        String entityName = getEntityName();
        int end = Math.min(3, entityName.length());
        String prefix = entityName.substring(0, end).toLowerCase();
        return prefix + getUserId();
    }

    String getUserId();

    List<String> getNames();

    String getEntityName();

    String getSocialName();

}
