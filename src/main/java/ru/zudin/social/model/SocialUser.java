package ru.zudin.social.model;

import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public interface SocialUser {

    long getUserId();

    List<String> getNames();

    String getEntityName();

    String getSocialName();

}
