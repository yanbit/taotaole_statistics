package state;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import function.YundbFunction2;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import storm.trident.TridentTopology;
import util.Constants;
import util.KafkaUtils;

/**
 * User: yanbit
 * Date: 2016/1/14
 * Time: 16:55
 */
public class YundbState {

    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();
        // proccess log
        topology.newStream("spout", KafkaUtils.getTopic("test-taotaole-jdbc-zz_yundb"))
                .each(new Fields("str"), new YundbFunction2(), new Fields(
                        "id",
                        "order_id",
                        "mid",
                        "username",
                        "buy_id",
                        "goods_name",
                        "goods_price",
                        "price",
                        "qty",
                        "total",
                        "cover",
                        "qishu",
                        "yun_code",
                        "luck_code",
                        "db_time",
                        "timenum",
                        "status",
                        "ip",
                        "is_show",
                        "is_award",
                        "add_time",
                        "type",
                        "sharecode",
                        "fdis",
                        "agents"))
                .partitionPersist(KafkaUtils.getSyncTableStateFactory("zz_yundb"), new Fields(
                        "id",
                        "order_id",
                        "mid",
                        "username",
                        "buy_id",
                        "goods_name",
                        "goods_price",
                        "price",
                        "qty",
                        "total",
                        "cover",
                        "qishu",
                        "yun_code",
                        "luck_code",
                        "db_time",
                        "timenum",
                        "status",
                        "ip",
                        "is_show",
                        "is_award",
                        "add_time",
                        "type",
                        "sharecode",
                        "fdis",
                        "agents"), new JdbcUpdater(), new Fields());

        // submit job
        Config config = new Config();
        config.put("jdbc.conf", Constants.getDBInfo());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology.build());
//            StormSubmitter.submitTopology("test_goods", config, topology.build());
    }
}
