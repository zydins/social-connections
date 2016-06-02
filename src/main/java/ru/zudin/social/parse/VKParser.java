package ru.zudin.social.parse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import ru.zudin.social.model.VKUser;
import twitter4j.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author sergey
 * @since 01.06.16
 */
public class VKParser implements SocialParser {

    private final static String DEFAULT_URL = "https://api.vk.com/method/{METHOD_NAME}?{PARAMETERS}";

    public List<VKUser> parse(long userId, int depth) throws IOException {
        String url = changeUrl(userId);
        List<Map<String, Object>> result = executeAndGet(url);
        List<VKUser> users = result.stream()
                .map(this::mapToUser)
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

    private VKUser mapToUser(Map<String, Object> map) {
        VKUser user = new VKUser();
        //required
        user.userId = (Integer) map.get("user_id");
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

    private String changeUrl(long userId) {
        String param = "user_id="+userId+"&fields=domain,connections,nickname";
        String url = DEFAULT_URL.replace("{METHOD_NAME}", "friends.get");
        return url.replace("{PARAMETERS}", param);
    }

    public static void main(String[] args) throws IOException, TwitterException {
        VKParser vkParser = new VKParser();
        List<VKUser> parse = vkParser.parse(32030584, 0);
        Map<Boolean, List<VKUser>> collect = parse.stream()
                .collect(Collectors.groupingBy(u -> u.twitter != null));
        System.out.println(collect);
    }

}
