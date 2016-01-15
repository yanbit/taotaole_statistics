import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Maps;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.UUID;

/**
 * User: yanbit
 * Date: 2016/1/14
 * Time: 16:55
 */
public class StormStateDB2 {

    public static void main(String[] args) throws Exception {
        Map map = Maps.newHashMap();
//        map.put("dataSourceClassName", args[0]);//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
//        map.put("dataSource.url", args[1]);//jdbc:mysql://localhost/test
//        map.put("dataSource.user", args[2]);//root
//        map.put("dataSource.password", args[3]);//password
        map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        map.put("dataSource.url", "jdbc:mysql://10.1.3.59/test");//jdbc:mysql://localhost/test
        map.put("dataSource.user", "root");//root
        map.put("dataSource.password", "hadoop");//password
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        String tableName = "test_goods";//database table name
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        Config config = new Config();
        config.put("jdbc.conf", map);

        BrokerHosts hosts = new ZkHosts("datanode1:2181,datanode2:2181,datanode4:2181");
        TridentKafkaConfig tridentKafkaConfig =
                new TridentKafkaConfig(hosts, "test_goods", UUID.randomUUID().toString());
        //tridentKafkaConfig.ignoreZkOffsets = true;
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout", tridentKafkaSpout)
                .each(new Fields("str"),new Function() {
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                        String line = tuple.getString(0);
                        String[] lines = line.split(",");
                        collector.emit(new Values(Integer.valueOf(lines[0]),lines[1], lines[2]));
                    }

                    public void prepare(Map conf, TridentOperationContext context) {

                    }

                    public void cleanup() {

                    }
                },new Fields("id","user_name","create_date"));

        JdbcState.Options options = new JdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(jdbcMapper)
                .withTableName(tableName);

        JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
        stream.partitionPersist(jdbcStateFactory, new Fields("id", "user_name", "create_date"), new JdbcUpdater(), new Fields());
//        if (args.length == 5) {
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("test", config, topology.build());
//            Thread.sleep(30000);
//            cluster.killTopology("test");
//            cluster.shutdown();
//            System.exit(0);
//        } else if (args.length == 6) {
            StormSubmitter.submitTopology("test_goods", config, topology.build());
//        } else {
//            System.out.println("Usage: UserPersistanceTopology <dataSourceClassName> <dataSource.url> "+
//                    "<user> <password> <tableName> [topology name]");
//        }
    }
}
