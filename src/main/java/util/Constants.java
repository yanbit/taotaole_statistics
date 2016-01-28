package util;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * User: yanbit
 * Date: 2016/1/15
 * Time: 16:26
 */
public class Constants {
//    public static final String ZOOKEEPER_LIST = "datanode1:2181,datanode2:2181,datanode4:2181";
//    public static final String BROKER_LIST = "namenode:9092,datanode1:9092,datanode4:9092";
    public static final String ZOOKEEPER_LIST = "datanode4:2181";
    public static final String BROKER_LIST = "datanode4:9092";
    public static Map getDBInfo(){
        Map map = Maps.newHashMap();
        map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        map.put("dataSource.url", "jdbc:mysql://10.1.3.59/yungou");
        map.put("dataSource.user", "root");
        map.put("dataSource.password", "hadoop");
        return map;
    }
}
