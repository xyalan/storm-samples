package com.hialan.wordCount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 3/26/15 13:28
 */
public class WordSpitBolt implements IRichBolt {
	private OutputCollector collector;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		String sentence = tuple.getString(0);
		String[] words = sentence.split(" ");
		Arrays.stream(words).forEach(word -> {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		});
		collector.ack(tuple);
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
