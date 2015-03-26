package com.hialan.wordCount.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 3/26/15 12:58
 */
public class LineReaderSpout implements IRichSpout {
	private SpoutOutputCollector collector;

	private FileReader fileReader;

	private boolean completed = false;

	private TopologyContext context;

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("line"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.context = context;
		this.collector = spoutOutputCollector;
		try {
			this.fileReader = new FileReader(map.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str), str);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			completed = true;
		}
	}

	public void ack(Object o) {

	}

	public void fail(Object o) {

	}
}
