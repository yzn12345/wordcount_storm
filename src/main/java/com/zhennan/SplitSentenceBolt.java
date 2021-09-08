package com.zhennan;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


//第一个bolt 切分单词
public class SplitSentenceBolt implements IRichBolt {
    private OutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    //Declare configuration specific to this component.
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    //Called when a task for this component is initialized within a worker on the cluster.
    // It provides the bolt with the environment in which the bolt executes.
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    //执行切分自定义任务
    //Process a single tuple of input. The Tuple object contains metadata on it about which component/stream/task it came from. The values of the Tuple can be accessed using Tuple#getValue. The IBolt does not have to process the Tuple immediately. It is perfectly fine to hang onto a tuple and process it later (for instance, to do an aggregation or join).
    //
    //Tuples should be emitted using the OutputCollector provided through the prepare method. It is required that all input tuples are acked or failed at some point using the OutputCollector. Otherwise, Storm will be unable to determine when tuples coming off the spouts have been completed.
    //
    //For the common case of acking an input tuple at the end of the execute method, see IBasicBolt which automates this.
    public void execute(Tuple input) {
        String sentence = input.getStringByField("word");
        String[] words = sentence.split(" ");
        for(String word : words){
            this.collector.emit(new Values(word));
        }
    }

    @Override
    public void cleanup() {

    }

}