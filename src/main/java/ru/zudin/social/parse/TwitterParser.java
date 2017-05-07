package ru.zudin.social.parse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.TwitterUser;
import twitter4j.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
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
        try {
            TwitterUser rootUser = mapToUser(twitter.users().showUser(userId));
            System.out.println("Start user " + rootUser.nickname);
            List<TwitterUser> following = getConnection(userId, getSafeFollowing());
            System.out.println("Following " + following.size() + " for user " + rootUser.nickname);
            List<TwitterUser> followers = new ArrayList<>();
            if (!following.isEmpty()) {
                followers = getConnection(userId, getSafeFollowers());
            }
            System.out.println("Followers " + followers.size() + " for user " + rootUser.nickname);
            List<Long> followersIds = followers.stream()
                    .map(u -> u.userId)
                    .collect(Collectors.toList());

            List<TwitterUser> users = following.stream()
                    .filter(u -> followersIds.contains(u.userId))
                    .collect(Collectors.toList());
            rootUser.addFriends(users);
            users.forEach(u -> u.addFriends(Collections.singletonList(rootUser)));
            System.out.println("Found " + users.size() + " for user " + rootUser.nickname);
            if (depth > 0) {
                List<TwitterUser> newUsers = users.stream()
                        .filter(u -> !u.nickname.equals("zudins"))
                        .map(u -> {
                            try {
                                List<TwitterUser> parse = parse(u.userId, depth - 1);
                                u.addFriends(parse);
                                parse.forEach(u2 -> u2.addFriends(Collections.singletonList(u)));
                                return parse;
                            } catch (IOException e) {
                                return null;
                            }
                        })
                        .filter(l -> l != null)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
                users.addAll(newUsers);
            }
            users.add(rootUser);
            return new ArrayList<>(users);
        } catch (TwitterException e) {
            throw new IOException();
        }
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowers() {
        return (l1, l2) -> {
            while (true) {
                try {
                    return twitter.friendsFollowers().getFollowersList(l1, l2, 200);
                } catch (TwitterException e) {
                    try {
                        String stackTrace = ExceptionUtils.getStackTrace(e);
                        if (stackTrace.startsWith("401")) {
                            //protected
                            return null;
                        }
                        String seconds = stackTrace.split("secondsUntilReset=")[1].split("}")[0];
                        System.out.println(new Date() + ": Stop execution for " + seconds);
                        int duration = Integer.parseInt(seconds);
                        if (duration > 0) {
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(duration));
                        }
                    } catch (Exception e2) {
                        System.out.println(e2);
                        return null;
                    }
                } catch (Exception e) {
                    System.out.println(e);
                    return null;
                }
            }
        };
    }

    private BiFunction<Long, Long, PagableResponseList<User>> getSafeFollowing() {
        return (l1, l2) -> {
            while (true) {
                try {
                    return twitter.friendsFollowers().getFriendsList(l1, l2, 200);
                } catch (TwitterException e) {
                    try {
                        String stackTrace = ExceptionUtils.getStackTrace(e);
                        if (stackTrace.startsWith("401")) {
                            //protected
                            return null;
                        }
                        String seconds = stackTrace.split("secondsUntilReset=")[1].split("}")[0];
                        System.out.println(new Date() + ": Stop execution for " + seconds);
                        int duration = Integer.parseInt(seconds);
                        if (duration > 0) {
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(duration));
                        }
                    } catch (Exception e2) {
                        System.out.println(e2);
                        return null;
                    }
                } catch (Exception e) {
                    System.out.println(e);
                    return null;
                }
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
//        main2();
    }

    private static void main2() {
        try {
            TwitterParser twitterParser = new TwitterParser();
            Optional<Long> id = twitterParser.getId("zydins");

            Collection<TwitterUser> parse = twitterParser.parse(id.get(), 1);
            Map<String, Optional<TwitterUser>> collect = parse.stream()
                    .collect(Collectors.groupingBy(SocialUser::getGlobalId,
                            Collectors.reducing((u1, u2) -> {
                                u1.friends.addAll(u2.friends);
                                return u1;
                            })));
            parse = collect.values().stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                    .collect(Collectors.toList());
            System.out.println(parse.size());

            PrintWriter totpw = new PrintWriter(new FileWriter("input/twitter.txt"));
            ObjectMapper mapper = new ObjectMapper();
            for (TwitterUser user : parse) {
                PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("input/friends/friends_" + user.getGlobalId() + ".txt", true)));
                for (TwitterUser friend : user.friends) {
                    pw.write(user.getGlobalId() + "\t" + friend.getGlobalId() + "\n");
                }
                pw.close();
                totpw.write(user.getGlobalId() + "\t" + user.getEntityName() + "\t" + mapper.writeValueAsString(user) + "\n");
            }
            totpw.close();
            System.out.println(1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }

    private static void parseUsers() {
        try {
            TwitterParser twitterParser = new TwitterParser();

            List<String> lines = Files.lines(Paths.get("input/users.txt"))
                    .filter(l -> l.startsWith("vk"))
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
                    if (!nickname.equals("zydins")) {
                        nicknameMap.put(nickname, l);
                    }
                }
            }

//            List<String> nicknames = lines.stream().
//                    map(l -> l.split("twitter\":")[1])
//                    .map(l -> l.split(",")[0])
//                    .filter(l -> !l.equals("null"))
//                    .map(l -> l.replace("\"", ""))
//                    .collect(Collectors.toList());

            List<TwitterUser> users = twitterParser.getSafeUsers(new ArrayList<>(nicknameMap.keySet()));

            System.out.println("Found "+users.size()+" users");
            AtomicInteger i = new AtomicInteger();
            List<TwitterUser> collect = users.stream()
                    .map(u -> {
                        try {
                            System.out.println(i.incrementAndGet());
                            return twitterParser.parse(u.userId, 0);
                        } catch (IOException e) {
                            return Collections.<TwitterUser>emptyList();
                        }
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            users.addAll(collect);
            Map<String, Optional<TwitterUser>> collect2 = users.stream()
                    .collect(Collectors.groupingBy(SocialUser::getGlobalId,
                            Collectors.reducing((u1, u2) -> {
                                u1.friends.addAll(u2.friends);
                                return u1;
                            })));
            users = collect2.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            System.out.println(users.size());

//            Map<String, TwitterUser> userNameMap = users.stream()
//                    .collect(Collectors.toMap(u -> u.nickname, Function.<TwitterUser>identity()));
//
//            Sets.SetView<String> foundNames = Sets.intersection(userNameMap.keySet(), nicknameMap.keySet());
//
//            users = foundNames.stream()
//                    .map(userNameMap::get)
//                    .collect(Collectors.toSet());
//            List<String> linesToProcess = foundNames.stream()
//                    .map(nicknameMap::get)
//                    .filter(n -> n != null)
//                    .collect(Collectors.toList());

//            List<String> toInsert = linesToProcess.stream()
//                    .map(l -> l.replaceAll("\"twitter\":.+,", "\"twitter\":null,"))
//                    .collect(Collectors.toList());

//            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("input/test4_twi1.txt", true)));
//            ObjectMapper mapper = new ObjectMapper();
////            for (String s : toInsert) {
////                pw.write(s + "\n");
////            }
//            for (TwitterUser user : users) {
//                pw.write(user.getGlobalId() + "\t" + user.getEntityName() + "\t" + mapper.writeValueAsString(user) + "\n");
//            }

            PrintWriter totpw = new PrintWriter(new FileWriter("input/test4_twi1.txt"));
            ObjectMapper mapper = new ObjectMapper();
            for (TwitterUser user : users) {
                PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("input/friends/friends_" + user.getGlobalId() + ".txt", true)));
                for (TwitterUser friend : user.friends) {
                    pw.write(user.getGlobalId() + "\t" + friend.getGlobalId() + "\n");
                }
                pw.close();
                totpw.write(user.getGlobalId() + "\t" + user.getEntityName() + "\t" + mapper.writeValueAsString(user) + "\n");
            }
            totpw.close();
            System.out.println(1);

//            pw.close();
//            System.out.println(1);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
