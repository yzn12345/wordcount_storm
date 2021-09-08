package com.zhennan;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    //spout输出收集器
    SpoutOutputCollector collector;
    Random random;


    //Called when a task for this component is initialized within a worker on the cluster.
    // It provides the spout with the environment in which the spout executes.
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
    }

    //When this method is called, Storm is requesting that the Spout emit tuples to the output collector.
    // This method should be non-blocking, so if the Spout has no tuples to emit, this method should return.
    // nextTuple, ack, and fail are all called in a tight loop in a single thread in the spout task.
    // When there are no tuples to emit,
    // it is courteous to have nextTuple sleep for a short amount of time (like a single millisecond)
    // so as not to waste too much CPU.
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[]{
                sentence("I have a dream"),
                sentence("my dream is to be a data analyst"),
                sentence("don't give up your dreams"),
                sentence("snow white and the seven dwarfs"),
                sentence("i am at two with nature")};
        final String sentence = sentences[random.nextInt(sentences.length)];

        collector.emit(new Values(sentence));
    }

    //helper method
    protected String sentence(String input) {
        return input;
    }

    //Declare the output schema for all the streams of this topology.
    //declarer - this is used to declare output stream ids, output fields,
    // and whether or not each output stream is a direct stream
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}

