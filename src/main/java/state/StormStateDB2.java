package state;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import function.GoodsFunction;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import storm.trident.TridentTopology;
import util.Constants;
import util.KafkaUtils;

/**
 * User: yanbit
 * Date: 2016/1/14
 * Time: 16:55
 */
public class StormStateDB2 {

    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();
        // proccess log
        topology.newStream("spout", KafkaUtils.getTopic("test"))
                .each(new Fields("str"), new GoodsFunction(), new Fields("id", "user_name", "create_date"))
                .partitionPersist(KafkaUtils.getSyncTableStateFactory("test_goods"), new Fields("id", "user_name", "create_date"), new JdbcUpdater(), new Fields());

        // submit job
        Config config = new Config();
        config.put("jdbc.conf", Constants.getDBInfo());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology.build());
//            StormSubmitter.submitTopology("test_goods", config, topology.build());
    }
}
