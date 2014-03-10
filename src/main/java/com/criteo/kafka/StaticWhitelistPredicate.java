package com.criteo.kafka;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import java.util.HashSet;
import java.util.List;

public class StaticWhitelistPredicate implements MetricPredicate {

    HashSet<String> whitelisted;

    public StaticWhitelistPredicate(List<String> whitelisted){
        this.whitelisted = new HashSet<String>(whitelisted);
    }

    @Override
    public boolean matches(MetricName name, Metric metric) {
        return whitelisted.contains(name.getName());
    }
}
