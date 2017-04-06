package ru.zudin.social.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.TokenizedUser;
import ru.zudin.social.util.HashUtils;
import ru.zudin.social.util.ParseHelper;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author sergey
 * @since 04.06.16
 */
public class UserMatchReducer extends Reducer<LongWritable, Text, Text, Text> {

    private final ParseHelper parseHelper;

    public UserMatchReducer() {
        parseHelper = new ParseHelper();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> texts = new ArrayList<>();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text next = iterator.next();
            texts.add(next.toString());
        }

        List<SocialUser> users = texts.stream()
                .map(parseHelper::readUser)
                .filter(u -> u != null)
                .collect(toList());
        Map<String, Set<SocialUser>> collect = users.stream()
                .collect(Collectors.groupingBy(SocialUser::getEntityName, Collectors.toSet()));
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
                                for (String name2 : secondTokens.keySet()) {
                                    if (name.equals(name2)) {
                                        probability = 1.0;
                                    } else {
                                        Set<String> firstStrings = firstTokens.get(name);
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
                            }
                            if (probability > 0.0) {
                                String contextKey = tokenizedUser.user.getGlobalId() + "\t" +
                                        otherUser.user.getGlobalId();
                                String contextValue = String.format("%.4f", probability);
                                context.write(new Text(contextKey), new Text(contextValue));
//                                multipleOutputs.write(new Text(contextKey), new Text(contextValue), tokenizedUser.user.getGlobalId());
//                                multipleOutputs.write(new Text(contextKey), new Text(contextValue), otherUser.user.getGlobalId());
                            }
                        }
                    }
                }
            }

        }
    }

    private TokenizedUser getTokenized(SocialUser u) {
        Map<String, Set<String>> collect = u.getNames().stream()
                .collect(Collectors.toMap(Function.<String>identity(), s -> HashUtils.continuousShingling(s, 6)));
        return new TokenizedUser(u, collect);
    }
}
