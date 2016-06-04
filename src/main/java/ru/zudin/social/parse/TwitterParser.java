package ru.zudin.social.parse;

import ru.zudin.social.model.TwitterUser;
import twitter4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author sergey
 * @since 02.06.16
 */
public class TwitterParser implements SocialParser {

    private final Twitter twitter;

    public TwitterParser() {
        this.twitter = TwitterFactory.getSingleton();
    }

    @Override
    public List<TwitterUser> parse(long userId, int depth) throws IOException {
        List<TwitterUser> following = getConnection(userId, getSafeFollowing());
        List<TwitterUser> followers = getConnection(userId, getSafeFollowers());
        List<Long> followersIds = followers.stream()
                .map(u -> u.userId)
                .collect(Collectors.toList());

        List<TwitterUser> users = following.stream()
                .filter(u -> followersIds.contains(u.userId))
                .collect(Collectors.toList());
        if (depth > 0) {
            users.stream()
                    .map(u -> u.userId)
                    .map(id -> {
                        try {
                            return parse(id, depth - 1);
                        } catch (IOException e) {
                            return null;
                        }
                    })
                    .filter(l -> l != null)
                    .flatMap(Collection::stream)
                    .forEach(users::add);
        }
        return users;
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowers() {
        return (l1, l2) -> {
            try {
                return twitter.friendsFollowers().getFollowersList(l1, l2);
            } catch (TwitterException e) {
                return null;
            }
        };
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowing() {
        return (l1, l2) -> {
            try {
                return twitter.friendsFollowers().getFriendsList(l1, l2);
            } catch (TwitterException e) {
                return null;
            }
        };
    }

    private List<TwitterUser> getConnection(long userId, BiFunction<Long, Long, PagableResponseList<User>> function) {
        long cursor = -1;
        List<TwitterUser> users = new ArrayList<>();
        while (true) {
            PagableResponseList<User> responseList = function.apply(userId, cursor);
            responseList.getNextCursor();
            responseList.stream()
                    .map(this::mapToUser)
                    .forEach(users::add);
            if (responseList.hasNext()) {
                cursor = responseList.getNextCursor();
            } else {
                break;
            }
        }
        return users;
    }

    private TwitterUser mapToUser(User user) {
        TwitterUser twitterUser = new TwitterUser();
        twitterUser.userId = user.getId();
        twitterUser.url = user.getURLEntity().getExpandedURL();
        twitterUser.name = user.getName();
        twitterUser.nickname = user.getScreenName();
        twitterUser.info = user.getDescription();
        TwitterUser.parseOtherNickname(twitterUser);
        return twitterUser;
    }

    public long getId(String username) throws TwitterException {
        User user = twitter.users().showUser("zydins");
        return user.getId();
    }

    public static void main(String[] args) throws TwitterException, IOException {
        TwitterParser twitterParser = new TwitterParser();
        long id = twitterParser.getId("zydins");
        List<TwitterUser> users = twitterParser.parse(id, 0);
        System.out.println(users);
    }
}
