package ru.zudin.social.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sergey
 * @since 02.06.16
 */
public class HashUtils {

    public static List<Integer> shinglingHash(String string) {
        int length = string.length();
        List<Integer> hashes = new ArrayList<>();
        hashes.add(string.hashCode());
        while (--length >= 5) {
            List<String> shingling = shingling(string, length);
            shingling.stream()
                    .map(String::hashCode)
                    .forEach(hashes::add);
        }
        return hashes;
    }

    public static List<String> shingling(String string, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k cannot be less than 1");
        }
        String[] split = string.split(" ");
        List<String> result = new ArrayList<>();
        for (int i = 0; i < split.length + 1 - k; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < k; j++) {
                builder.append(split[i + j]).append(" ");
            }
            String phrase = builder.toString().trim();
            result.add(phrase);
        }
        return result;
    }

}
