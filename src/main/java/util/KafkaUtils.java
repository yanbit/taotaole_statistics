package util;

import backtype.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

import java.util.UUID;

/**
 * User: yanbit
 * Date: 2016/1/15
 * Time: 16:33
 */
public class KafkaUtils {
    public static TransactionalTridentKafkaSpout getTopic(String topicName){
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        TridentKafkaConfig tridentKafkaConfig =
                new TridentKafkaConfig(hosts, topicName, UUID.randomUUID().toString());
        //tridentKafkaConfig.ignoreZkOffsets = true;
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        return tridentKafkaSpout;
    }

    public static JdbcStateFactory getStateFactory(String tableName){
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(Constants.getDBInfo());
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        JdbcState.Options options = new JdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(jdbcMapper)
                .withTableName(tableName);
        JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
        return jdbcStateFactory;
    }
}
