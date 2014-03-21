package com.criteo.kafka;

import com.yammer.metrics.core.MetricName;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StaticWhitelistPredicateTest {
    @Test
    public void testMatches() throws Exception {
        List<String> allowed = Arrays.asList("a", "b");
        StaticWhitelistPredicate predicate = new StaticWhitelistPredicate(allowed);
        MetricName matched = new MetricName(String.class,"a");
        MetricName notmatched = new MetricName(String.class,"foo");
        assertTrue(predicate.matches(matched,null));
        assertFalse(predicate.matches(notmatched, null));
    }
}
