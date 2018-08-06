package afahmin.SampleTwitterRead;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class TopologyMain {
	 public static void main(String[] args) throws InterruptedException {
		 
		 //Topology definition
		 TopologyBuilder builder = new TopologyBuilder();
		 builder.setSpout("tweets-collector", new twitterSpout());
		 builder.setBolt("text-extractor", new extractStatusBolt()).shuffleGrouping("tweets-collector"); 
		 
		//Configuration
		 Config conf = new Config();
		 conf.setDebug(true);
		 conf.put("dirToWrite", "E:/eclipse/SampleTwitterRead/");
    
    
		 LocalCluster cluster = new LocalCluster();
		 cluster.submitTopology("twitter-direct", conf, builder.createTopology());
		 Thread.sleep(10000);
		 cluster.shutdown();
	 }
}
