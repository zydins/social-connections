package ru.zudin.social.util;

import org.apache.commons.collections.map.MultiKeyMap;
import ru.zudin.social.model.SocialName;

import static ru.zudin.social.model.SocialNetwork.Twitter;
import static ru.zudin.social.model.SocialNetwork.VK;

/**
 * @author sergey
 * @since 06.04.17
 */
public class ProbabilityHelper {

    private final MultiKeyMap map;

    public ProbabilityHelper() {
        map = new MultiKeyMap();
        map.put(new SocialName("domain", VK), new SocialName("nickname", Twitter), 0.8);
        map.put(new SocialName("skype", VK), new SocialName("nickname", Twitter), 0.7);
        map.put(new SocialName("facebook", VK), new SocialName("nickname", Twitter), 0.7);
        map.put(new SocialName("twitter", VK), new SocialName("nickname", Twitter), 1.0);
        map.put(new SocialName("instagram", VK), new SocialName("nickname", Twitter), 0.7);
        map.put(new SocialName("livejournal", VK), new SocialName("nickname", Twitter), 0.7);

        map.put(new SocialName("domain", VK), new SocialName("otherNickname", Twitter), 0.8);
        map.put(new SocialName("skype", VK), new SocialName("otherNickname", Twitter), 0.7);
        map.put(new SocialName("facebook", VK), new SocialName("otherNickname", Twitter), 0.7);
        map.put(new SocialName("twitter", VK), new SocialName("otherNickname", Twitter), 0.7);
        map.put(new SocialName("instagram", VK), new SocialName("otherNickname", Twitter), 0.7);
        map.put(new SocialName("livejournal", VK), new SocialName("otherNickname", Twitter), 0.7);
    }

    public double getProbability(SocialName name1, SocialName name2) {
        SocialName first = name1.network == VK ? name1 : name2;
        SocialName second = name1.network == Twitter ? name1 : name2;
        Object o = map.get(first, second);
        if (o == null) {
            return 0.2;
        } else {
            return (Double) o;
        }
    }

}
