package ru.zudin.social.parse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ru.zudin.social.model.TwitterUser;
import twitter4j.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
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
        try {
            TwitterUser rootUser = mapToUser(twitter.users().showUser(userId));
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
            users.add(rootUser);
            return users;
        } catch (TwitterException e) {
            throw new IOException();
        }
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowers() {
        return (l1, l2) -> {
            try {
                return twitter.friendsFollowers().getFollowersList(l1, l2);
            } catch (TwitterException e) {
                System.out.println(e);
                return null;
            }
        };
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowing() {
        return (l1, l2) -> {
            try {
                return twitter.friendsFollowers().getFriendsList(l1, l2);
            } catch (TwitterException e) {
                System.out.println(e);
                return null;
            }
        };
    }

    private List<TwitterUser> getConnection(long userId, BiFunction<Long, Long, PagableResponseList<User>> function) {
        long cursor = -1;
        List<TwitterUser> users = new ArrayList<>();
        while (true) {
            PagableResponseList<User> responseList = function.apply(userId, cursor);
            if (responseList != null) {
                responseList.getNextCursor();
                responseList.stream()
                        .map(this::mapToUser)
                        .forEach(users::add);
                if (responseList.hasNext()) {
                    cursor = responseList.getNextCursor();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return users;
    }

    public Optional<TwitterUser> getUser(String username) throws TwitterException {
        User user = twitter.users().showUser(username);
        if (user != null) {
            return Optional.of(mapToUser(user));
        } else {
            return Optional.empty();
        }
    }

    public Optional<TwitterUser> getSafeUser(String username) {
        try {
            return getUser(username);
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    public List<TwitterUser> getSafeUsers(List<String> usernames) {
        try {
            List<TwitterUser> res = new ArrayList<>();
            List<List<String>> split = Lists.partition(usernames, 100);
            for (List<String> strings : split) {
                ResponseList<User> users = twitter.users().lookupUsers(strings.toArray(new String[0]));
                List<TwitterUser> list = users.stream()
                        .map(this::mapToUser)
                        .collect(Collectors.toList());
                res.addAll(list);
            }
            return res;
        } catch (Throwable t) {
            return Collections.emptyList();
        }
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

    public Optional<Long> getId(String username) throws TwitterException {
        User user = twitter.users().showUser(username);
        if (user == null) {
            return Optional.empty();
        }
        return Optional.of(user.getId());
    }

    public static void main(String[] args) throws TwitterException, IOException {
        parseUsers();
    }

    private static void parseUsers() {
        try {
            TwitterParser twitterParser = new TwitterParser();

            List<String> lines = Files.lines(Paths.get("input/vk.txt"))
//                    .filter(l -> l.startsWith("vk"))
//                    .map(l -> l.split("twitter\":")[1])
//                    .map(l -> l.split(",")[0])
//                    .filter(l -> !l.equals("null"))
//                    .map(l -> l.replace("\"", ""))
                    .distinct()
                    .filter(l -> !l.contains("\"twitter\":null"))
                    .collect(Collectors.toList());

            Map<String, String> nicknameMap = new HashMap<>();
            for (String l : lines) {
                String nickname = l.split("twitter\":")[1].split(",")[0];
                if (!nickname.equals("null")) {
                    nickname = nickname.replace("\"", "");
                    nicknameMap.put(nickname, l);
                }
            }

//            List<String> nicknames = lines.stream().
//                    map(l -> l.split("twitter\":")[1])
//                    .map(l -> l.split(",")[0])
//                    .filter(l -> !l.equals("null"))
//                    .map(l -> l.replace("\"", ""))
//                    .collect(Collectors.toList());

            Set<TwitterUser> users = new HashSet<>(twitterParser.getSafeUsers(new ArrayList<>(nicknameMap.keySet())));

//            System.out.println("Found "+users.size()+" users");
//            AtomicInteger i = new AtomicInteger();
//            List<TwitterUser> collect = users.stream()
//                    .map(u -> {
//                        try {
//                            if (i.incrementAndGet() % 100 == 0) {
//                                System.out.println(i);
//                            }
//                            return twitterParser.parse(u.userId, 0);
//                        } catch (IOException e) {
//                            return Collections.<TwitterUser>emptyList();
//                        }
//                    })
//                    .flatMap(Collection::stream)
//                    .collect(Collectors.toList());
//            users.addAll(collect);

            Map<String, TwitterUser> userNameMap = users.stream()
                    .collect(Collectors.toMap(u -> u.nickname, Function.<TwitterUser>identity()));

            Sets.SetView<String> foundNames = Sets.intersection(userNameMap.keySet(), nicknameMap.keySet());

            users = foundNames.stream()
                    .map(userNameMap::get)
                    .collect(Collectors.toSet());
            List<String> linesToProcess = foundNames.stream()
                    .map(nicknameMap::get)
                    .filter(n -> n != null)
                    .collect(Collectors.toList());

            List<String> toInsert = linesToProcess.stream()
                    .map(l -> l.replaceAll("\"twitter\":.+,", "\"twitter\":null,"))
                    .collect(Collectors.toList());

            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("input/test1.txt", true)));
            ObjectMapper mapper = new ObjectMapper();
            for (String s : toInsert) {
                pw.write(s + "\n");
            }
            for (TwitterUser user : users) {
                pw.write(user.getGlobalId() + "\t" + user.getEntityName() + "\t" + mapper.writeValueAsString(user) + "\n");
            }

            pw.close();
            System.out.println(1);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
