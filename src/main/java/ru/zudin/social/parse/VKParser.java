package ru.zudin.social.parse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.VKUser;
import twitter4j.TwitterException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author sergey
 * @since 01.06.16
 */
public class VKParser implements SocialParser {

    private final static String DEFAULT_URL = "https://api.vk.com/method/{METHOD_NAME}?{PARAMETERS}";

    public List<VKUser> parse(long userId, int depth) throws IOException {
        List<Map<String, Object>> maps = executeAndGet(getUserUrl(userId));
        if (maps == null || maps.size() != 1) {
            throw new IOException();
        }
        VKUser rootUser = mapToUser(maps.get(0));
        String url = friendsGetUrl(userId);
        List<Map<String, Object>> result = executeAndGet(url);
        List<VKUser> users = result.stream()
                .map(this::mapToUser)
                .collect(Collectors.toList());
        rootUser.addFriends(users);
        users.forEach(u -> u.addFriends(Collections.singletonList(rootUser)));

        if (depth > 0) {
            List<VKUser> collect = users.stream()
                    .map(u -> {
                        try {
                            List<VKUser> parse = parse(u.userId, depth - 1);
                            u.addFriends(parse);
                            parse.forEach(u2 -> u2.addFriends(Collections.singletonList(u)));
                            return parse;
                        } catch (IOException e) {
                            return Collections.<VKUser>emptyList();
                        }
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            users.addAll(collect);
        }
        users.add(rootUser);
        return users;
    }

    private VKUser mapToUser(Map<String, Object> map) {
        VKUser user = new VKUser();
        //required
        user.userId = (Integer) map.getOrDefault("uid", map.getOrDefault("id", map.getOrDefault("user_id", null)));
        user.domain = (String) map.get("domain");
        user.firstName = (String) map.get("first_name");
        user.lastName = (String) map.get("last_name");
        //optional
        user.facebook = (String) map.get("facebook");
        user.facebookName = (String) map.get("facebookName");
        user.skype = (String) map.get("skype");
        user.twitter = (String) map.get("twitter");
        user.livejournal = (String) map.get("livejournal");
        user.instagram = (String) map.get("instagram");

        return user;
    }

    private List<Map<String, Object>> executeAndGet(String url) throws IOException {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        List<Map<String, Object>> result = null;
        try {
            client = HttpClientBuilder.create().build();
            HttpGet httpGet = new HttpGet(url);
            response = client.execute(httpGet);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, List<Map<String, Object>>> map = objectMapper.readValue(response.getEntity().getContent(), new TypeReference<Map<String, List<Map<String, Object>>>>() {
            });
            result = map.get("response");
        } finally {
            if (client != null) {
                client.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    private String getUserUrl(long userId) {
        String param = "user_ids="+userId+"&fields=domain,connections,nickname";
        String url = DEFAULT_URL.replace("{METHOD_NAME}", "users.get");
        return url.replace("{PARAMETERS}", param);
    }

    private String friendsGetUrl(long userId) {
        String param = "user_id="+userId+"&fields=domain,connections,nickname";
        String url = DEFAULT_URL.replace("{METHOD_NAME}", "friends.get");
        return url.replace("{PARAMETERS}", param);
    }

    public static void main(String[] args) throws IOException, TwitterException {
        VKParser vkParser = new VKParser();
//        List<VKUser> parse = vkParser.parse(32030584, 1);

        List<Integer> toParse = new ArrayList<>(Arrays.asList(34720084, 23331259, 5485951, 25208563, 2616669));
        Files.lines(Paths.get("input/users.txt"))
                .filter(l -> l.startsWith("vk"))
                .distinct()
                .filter(l -> !l.contains("\"twitter\":null"))
                .map(l -> l.split("\t")[0])
                .map(l -> l.replace("vk", ""))
                .map(Integer::parseInt)
                .forEach(toParse::add);

        AtomicInteger i = new AtomicInteger();
        List<VKUser> parse = toParse.stream()
                .map(u -> {
                    try {
                        System.out.println(i.incrementAndGet());
                        return vkParser.parse(u, 0);
                    } catch (IOException e) {
                        return Collections.<VKUser>emptyList();
                    }
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Map<String, Optional<VKUser>> collect2 = parse.stream()
                .collect(Collectors.groupingBy(SocialUser::getGlobalId,
                        Collectors.reducing((u1, u2) -> {
                            u1.friends.addAll(u2.friends);
                            return u1;
                        })));
        parse = collect2.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        PrintWriter totpw = new PrintWriter(new FileWriter("input/test4_vk1.txt"));
        ObjectMapper mapper = new ObjectMapper();
        for (VKUser user : parse) {
            PrintWriter pw = new PrintWriter(new FileWriter("input/friends/friends_" + user.getGlobalId() + ".txt"));
            for (VKUser friend : user.friends) {
                pw.write(user.getGlobalId() + "\t" + friend.getGlobalId() + "\n");
            }
            pw.close();
            totpw.write(user.getGlobalId() + "\t" + user.getEntityName() + "\t" + mapper.writeValueAsString(user) + "\n");
        }
        totpw.close();
        System.out.println(1);

    }

}
