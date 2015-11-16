package test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author yanbit
 * @date Nov 16, 2015 3:15:54 PM
 * @todo TODO
 */
public class TestBolt extends BaseRichBolt {

  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    try {
      String line = input.getString(0);
      System.out.println("===================line:"+line);
      collector.ack(input);
    } catch (Exception e) {
      collector.fail(input);
      e.printStackTrace();
    }
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
