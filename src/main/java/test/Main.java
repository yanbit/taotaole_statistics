package test;

import com.google.common.collect.Iterators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import taotaole.avro.zz_yundb;

import java.io.File;
import java.io.IOException;

/**
 * User: yanbit
 * Date: 2016/1/25
 * Time: 18:11
 */
public class Main {
    public static void main(String[] args) throws IOException {
        //String line = "{\"id\":1304,\"order_id\":1240,\"mid\":100000020,\"username\":\"566c5598a0051\",\"buy_id\":217,\"goods_name\":\"海购商品 2件组合装 | Nature Republic 自然乐园 芦荟水乳套装 化妆水+乳液\",\"goods_price\":\"\\u0001ô\",\"price\":\"\\n\",\"qty\":39,\"total\":\"\\u0001\",\"cover\":60,\"qishu\":15,\"yun_code\":{\"string\":\"10000012,10000027,10000035,10000016,10000038,10000020,10000004,10000024,10000008,10000013,10000029,10000044,10000003,10000006,10000031,10000009,10000049,10000028,10000030,10000002,10000011,10000022,10000048,10000040,10000045,10000026,10000050,10000023,10000010,10000019,10000007,10000043,10000017,10000001,10000047,10000039,10000005,10000032,10000025\"},\"luck_code\":null,\"db_time\":\"1450319788.825\",\"timenum\":{\"string\":\"103628825\"},\"status\":5,\"ip\":\"10.1.39.116\",\"is_show\":1,\"is_award\":0,\"add_time\":1450319788,\"type\":{\"int\":1},\"sharecode\":\"\",\"fdis\":{\"int\":0},\"agents\":{\"string\":\"android\"}}";
        String line = "{\"id\":9298,\"order_id\":9191,\"mid\":100007330,\"username\":\"2865812291\",\"buy_id\":371,\"goods_name\":\"Chanel香奈儿男士香水 蔚蓝魅力男香2ml50ml100ml等 -Allure魅力运动2ml\",\"goods_price\":\"\\u0001Â\",\"price\":\"\\u0001\",\"qty\":3,\"total\":\"\\u0003\",\"cover\":0,\"qishu\":59,\"yun_code\":{\"string\":\"10000295,10000120,10000297\"},\"luck_code\":null,\"db_time\":\"1451160063.918\",\"timenum\":{\"string\":\"040103918\"},\"status\":5,\"ip\":\"111.254.116.235\",\"is_show\":1,\"is_award\":0,\"add_time\":1451160063,\"type\":{\"int\":1},\"sharecode\":\"\",\"fdis\":{\"int\":0},\"agents\":{\"string\":\"other\"}}\n";
        //Schema schema=Schema.parse(getClass().getResourceAsStream("ChangeLogContent.avsc"));
        Schema schema = new Schema.Parser().parse(new File("src/main/java/avro/zz_yundb.avro"));
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema,line);
        SpecificDatumReader<zz_yundb> din = new SpecificDatumReader<zz_yundb>(zz_yundb.class);
        GenericDatumReader<zz_yundb> datumReader = null;
        zz_yundb key = null;
        try {
            key = din.read(null,decoder);
//            System.out.println(key);
//            System.out.println(key.getId());
//            System.out.println(key.getOrderId());
//            System.out.println(key.getMid());
//            System.out.println(key.getUsername());
//            System.out.println(key.getBuyId());
//            System.out.println(key.getGoodsName());
//            System.out.println(new BigInteger(key.getGoodsPrice().array()));
//            System.out.println(new BigInteger(key.getPrice().array()));
//            System.out.println(key.getQty());
//            System.out.println(new BigInteger(key.getTotal().array()));
//            System.out.println(key.getCover());
//            System.out.println(Iterators.getLast(key.getYunCode().values().iterator()));
//            System.out.println(key.getLuckCode());
//            System.out.println(key.getDbTime());
//            System.out.println(key.getTimenum());
//            System.out.println(key.getStatus());
//            System.out.println(key.getIp());
//            System.out.println(key.getIsShow());
//            System.out.println(key.getIsAward());
//            System.out.println(key.getAddTime());
            System.out.println(Iterators.getLast(key.getType().values().iterator()));
            System.out.println(key.getSharecode());
            System.out.println(Iterators.getLast(key.getFdis().values().iterator()));
            System.out.println(Iterators.getLast(key.getAgents().values().iterator()));

        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
