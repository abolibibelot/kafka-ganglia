package com.criteo.kafka;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import java.util.regex.Pattern;

public class RegexMetricPredicate implements MetricPredicate {

	Pattern[] patterns = null;

	public RegexMetricPredicate(String[] regexes) {
        patterns = new Pattern[regexes.length];
        for(int i=0; i < regexes.length;i++)
		    patterns[i] = Pattern.compile(regexes[i]);
	}
	
	@Override
	public boolean matches(MetricName name, Metric metric) {
        //patterns are OR-ed (any match will return false)
        for (Pattern pattern : patterns) {
            boolean ok = ! pattern.matcher(name.getName()).matches();
            if (!ok)
                return false;
        }
            return true;
	}

}
