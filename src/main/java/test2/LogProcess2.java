package test2;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import test.TestBolt;

import java.util.Properties;
import java.util.UUID;
/**
 * @author yanbit
 * @date Nov 16, 2015 3:09:40 PM
 * @todo TODO
 */
public class LogProcess2 {
  public static void main(String[] args) throws InvalidTopologyException,
      AuthorizationException, AlreadyAliveException {
    TopologyBuilder builder = new TopologyBuilder();
    //TransactionalTopologyBuilder
    BrokerHosts hosts =
        new ZkHosts("datanode1:2181,datanode2:2181,datanode4:2181");
    String topic = "test_whx_1";
    String zkRoot = "/test_whx_1";
    String id = UUID.randomUUID().toString();
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.startOffsetTime=kafka.api.OffsetRequest.LatestTime();

    builder.setSpout("spout1", new KafkaSpout(spoutConfig),1);
    builder.setBolt("bolt1", new TestBolt2(), 1)
        .shuffleGrouping("spout1");

    Config config = new Config();
    Properties props = new Properties();
    props.put("metadata.broker.list",
        "10.1.3.55:9092,10.1.3.56:9092,10.1.3.59:9092");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
    config.setNumWorkers(1);
    StormSubmitter.submitTopology("testack2", config, builder.createTopology());
//    LocalCluster cluster = new LocalCluster();
//    cluster.submitTopology("testack3", config, builder.createTopology());
    
  }
}
