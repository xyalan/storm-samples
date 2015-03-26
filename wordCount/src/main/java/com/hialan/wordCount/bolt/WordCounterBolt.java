package com.hialan.wordCount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 3/26/15 13:34
 */
public class WordCounterBolt implements IRichBolt {
	private Map<String, Integer> counters;

	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.counters = new HashMap<>();
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		String str = tuple.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		counters.entrySet().stream().forEach(x -> System.out.println("Storm-Start实例子" + x.getKey() +
				":"	+ x
				.getValue()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
