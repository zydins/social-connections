package ru.zudin.social.mr;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.TokenizedUser;
import ru.zudin.social.util.HashUtils;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 04.06.16
 */
public class UserMatchReducer extends Reducer<LongWritable, SocialUser, Text, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<SocialUser> values, Context context) throws IOException, InterruptedException {
        List<SocialUser> users = Lists.newArrayList(values);
        Map<String, List<SocialUser>> collect = users.stream().collect(Collectors.groupingBy(SocialUser::getEntityName));
        if (collect.size() > 1) {
            List<List<TokenizedUser>> lists = collect.values().stream()
                    .map(l -> l.stream()
                            .map(this::getTokenized)
                            .collect(toList()))
                    .collect(toList());

            for (int i = 0; i < lists.size() - 1; i++) {
                List<TokenizedUser> socialGroup = lists.get(i);
                for (int j = i + 1; j < lists.size(); j++) {
                    List<TokenizedUser> otherGroup = lists.get(j);
                    for (TokenizedUser tokenizedUser : socialGroup) {
                        for (TokenizedUser otherUser : otherGroup) {
                            double probability = 0.0;
                            Map<String, Set<String>> firstTokens = tokenizedUser.tokens;
                            Map<String, Set<String>> secondTokens = otherUser.tokens;
                            for (String name : firstTokens.keySet()) {
                                Set<String> firstStrings = firstTokens.get(name);
                                for (String name2 : secondTokens.keySet()) {
                                    Set<String> secondStrings = secondTokens.get(name2);
                                    for (String string : firstStrings) {
                                        if (secondStrings.contains(string)) {
                                            double newProb = ((double) string.length() / name.length()) *
                                                    ((double) string.length() / name2.length());
                                            if (newProb > probability) {
                                                probability = newProb;
                                            }
                                        }
                                    }
                                }
                            }
                            if (probability > 0.0) {
                                String contextKey = tokenizedUser.user.getEntityName() + ":" + tokenizedUser.user.getId() + "\t" +
                                        otherUser.user.getEntityName() + ":" + otherUser.user.getId();
                                String contextValue = "Probability:" + String.format("%.4f", probability);
                                context.write(new Text(contextKey), new Text(contextValue));
                            }
                        }
                    }
                }
            }

        }
    }

    private TokenizedUser getTokenized(SocialUser u) {
        Map<String, Set<String>> collect = u.getNames().stream()
                .collect(Collectors.toMap(Function.<String>identity(), s -> HashUtils.continiousShingling(s, 5)));
        return new TokenizedUser(u, collect);
    }
}
