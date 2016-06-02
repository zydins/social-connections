package ru.zudin.social.parse;

import ru.zudin.social.model.SocialUser;

import java.io.IOException;
import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public interface SocialParser {

    List<? extends SocialUser> parse(long userId, int depth) throws IOException;

}
