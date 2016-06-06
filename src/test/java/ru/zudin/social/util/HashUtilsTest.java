package ru.zudin.social.util;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;

/**
 * @author sergey
 * @since 07.06.16
 */
public class HashUtilsTest {

    @Test
    public void testShinglingHash() throws Exception {
        List<Integer> integers = HashUtils.shinglingHash("anton.galaev");
        List<Integer> integers1 = HashUtils.shinglingHash("wgalaev");
        boolean disjoint = Collections.disjoint(integers, integers1);
        assertFalse(disjoint);
        System.out.println(11);
    }
}