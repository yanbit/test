package test2;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @author yanbit
 * @date Nov 16, 2015 4:12:05 PM
 * @todo TODO
 */
public class TestBolt2 implements IBasicBolt {

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("fields"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {

  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String line = null;
    try {
      line = input.getString(0);
      if (line.contains("Error")) {
        throw new RuntimeException();
      }
      Thread.sleep(1000);
      System.out.println("===================line:" + line);
      //collector.ack(input);
    } catch (Exception e) {
      System.out.println("===================Error line:" + line);
      //collector.fail(input);
      //collector.reportError(e);
      e.printStackTrace();
    }
  }

  @Override
  public void cleanup() {

  }

}
