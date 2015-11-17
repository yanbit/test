package com.dsk.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.hash.Hashing;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by user on 8/4/14.
 */
public class TestProducer2 {
  final static String TOPIC = "test_whx_1";

  public static void main(String[] argv) throws IOException {

    TestProducer2 t = new TestProducer2();
    while (true) {
      t.sendUpUserMessage();
    }
  }

  public void sendUpUserMessage() throws IOException {
    Properties properties = new Properties();
    properties.put("metadata.broker.list",
        "10.1.3.55:9092,10.1.3.56:9092,10.1.3.59:9092");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig producerConfig = new ProducerConfig(properties);
    kafka.javaapi.producer.Producer<String, String> producer =
        new kafka.javaapi.producer.Producer<String, String>(producerConfig);

    InputStream in = this.getClass().getResourceAsStream("/upusers.csv");
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // System.out.println(line);
      ArrayList<String> list = new ArrayList<String>(
          Arrays.asList(line.replace(";NULL", "").split(",")));
      if (list.size() != 0) {
        list.remove(0);
        String uid = list.remove(0);
        String nline = Hashing.md5().hashString(
            uid + System.currentTimeMillis() + new Random().nextLong(),
            Charsets.UTF_8) + ","
            + Joiner.on(",").join(list.toArray()).toString();
        // String nline = Joiner.on(",").join(list.toArray()).toString();
        KeyedMessage<String, String> message =
            new KeyedMessage<String, String>(TOPIC, nline);
        producer.send(message);
        // System.out.println(nline);
        //System.out.println(nline);
      }
    }
  }
}
