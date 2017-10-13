import bolts.WordNormal;
import bolts.WordReport;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.WordReader;

public class WordCountMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-nomal",new WordNormal()).shuffleGrouping("word-reader");
        builder.setBolt("word-report",new WordReport()).shuffleGrouping("word-nomal");

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LJW-TOPOLOGIES",conf,builder.createTopology());
        Thread.sleep(60000);
        localCluster.killTopology("LJW-TOPOLOGIES");
        localCluster.shutdown();

    }
}
