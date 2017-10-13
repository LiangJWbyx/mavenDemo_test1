package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "I am a Student",
            "I love my School"
    };
    private int index = 0;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String str;
        if (index < sentences.length){
            str = sentences[index];
            index++;
            collector.emit(new Values(str));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
