package ru.zudin.social.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author sergey
 * @since 02.06.16
 */
public class HashUtils {

    public static List<Integer> shinglingHash(String string) {
        int length = string.length();
        List<Integer> hashes = new ArrayList<>();
        hashes.add(string.hashCode());
        while (--length >= 6) {
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
        List<String> result = new ArrayList<>();
        for (int i = 0; i < string.length() + 1 - k; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < k; j++) {
                builder.append(string.charAt(i + j));
            }
            String phrase = builder.toString().trim();
            result.add(phrase);
        }
        return result;
    }

    public static Set<String> continiousShingling(String string, int limit) {
        int length = string.length();
        Set<String> shingles = new HashSet<>();
        while (--length >= limit) {
            List<String> shingling = shingling(string, length);
            shingles.addAll(shingling);
        }
        return shingles;
    }

}
