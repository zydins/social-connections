package ru.zudin.social.model;

import java.util.Map;
import java.util.Set;

/**
 * @author sergey
 * @since 04.06.16
 */
public class TokenizedUser {

    public SocialUser user;
    public Map<SocialName, Set<String>> tokens;

    public TokenizedUser(SocialUser user, Map<SocialName, Set<String>> tokens) {
        this.user = user;
        this.tokens = tokens;
    }
}
