package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordReport extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String,Long> counts = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if (count==null){
            count = 0L;
        }
        count++;
        counts.put(word,count);
    }

    @Override
    public void cleanup(){
        System.out.println("----word：----------count");
        for (Map.Entry<String,Long> entry:counts.entrySet()){
            System.out.println("--"+entry.getKey()+": "+entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
