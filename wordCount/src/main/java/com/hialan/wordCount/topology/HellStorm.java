package com.hialan.wordCount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.hialan.wordCount.bolt.WordCounterBolt;
import com.hialan.wordCount.bolt.WordSpitBolt;
import com.hialan.wordCount.spout.LineReaderSpout;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 3/26/15 12:57
 */
public class HellStorm {
	public static void main(String[] args) {
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("line-reader-spout", new LineReaderSpout());
		topologyBuilder.setBolt("word-spit", new WordSpitBolt()).shuffleGrouping
				("line-reader-spout");
		topologyBuilder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spit");

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("HelloStorm", config, topologyBuilder.createTopology());
		localCluster.shutdown();
	}
}
