package ru.zudin.social.model;

import org.apache.hadoop.io.Writable;

import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public interface SocialUser extends Writable {

    List<String> getNames();

    String getEntityName();

}
