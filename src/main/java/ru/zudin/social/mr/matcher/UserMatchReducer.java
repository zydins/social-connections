package ru.zudin.social.mr.matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.zudin.social.model.SocialName;
import ru.zudin.social.model.SocialNetwork;
import ru.zudin.social.model.SocialUser;
import ru.zudin.social.model.TokenizedUser;
import ru.zudin.social.util.HashUtils;
import ru.zudin.social.util.ParseHelper;
import ru.zudin.social.util.ProbabilityHelper;

import java.io.IOException;
import java.math.BigDecimal;
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
    private final ProbabilityHelper probabilityHelper;

    public UserMatchReducer() {
        this.probabilityHelper = new ProbabilityHelper();
        this.parseHelper = new ParseHelper();
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
        Map<SocialNetwork, Set<SocialUser>> collect = users.stream()
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
                            Map<SocialName, Set<String>> firstTokens = tokenizedUser.tokens;
                            Map<SocialName, Set<String>> secondTokens = otherUser.tokens;
                            for (SocialName name : firstTokens.keySet()) {
                                for (SocialName name2 : secondTokens.keySet()) {
                                    if (name.value.equals(name2.value)) {
                                        probability = probabilityHelper.getProbability(name, name2);
                                    } else {
                                        Set<String> firstStrings = firstTokens.get(name);
                                        Set<String> secondStrings = secondTokens.get(name2);
                                        for (String string : firstStrings) {
                                            if (secondStrings.contains(string)) {
                                                double newProb = ((double) string.length() / name.value.length()) *
                                                        ((double) string.length() / name2.value.length());
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
                                BigDecimal decimal = new BigDecimal(probability).setScale(4, BigDecimal.ROUND_HALF_UP);
                                context.write(new Text(contextKey), new Text(decimal.toString()));
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
        Map<SocialName, Set<String>> collect = u.getNames().stream()
                .collect(Collectors.toMap(Function.<SocialName>identity(), s -> HashUtils.continuousShingling(s.value, 6)));
        return new TokenizedUser(u, collect);
    }
}
