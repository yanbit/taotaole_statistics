package test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
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
        String line = "{\"id\":1304,\"order_id\":1240,\"mid\":100000020,\"username\":\"566c5598a0051\",\"buy_id\":217,\"goods_name\":\"海购商品 2件组合装 | Nature Republic 自然乐园 芦荟水乳套装 化妆水+乳液\",\"goods_price\":\"\\u0001ô\",\"price\":\"\\n\",\"qty\":39,\"total\":\"\\u0001\",\"cover\":60,\"qishu\":15,\"yun_code\":{\"string\":\"10000012,10000027,10000035,10000016,10000038,10000020,10000004,10000024,10000008,10000013,10000029,10000044,10000003,10000006,10000031,10000009,10000049,10000028,10000030,10000002,10000011,10000022,10000048,10000040,10000045,10000026,10000050,10000023,10000010,10000019,10000007,10000043,10000017,10000001,10000047,10000039,10000005,10000032,10000025\"},\"luck_code\":null,\"db_time\":\"1450319788.825\",\"timenum\":{\"string\":\"103628825\"},\"status\":5,\"ip\":\"10.1.39.116\",\"is_show\":1,\"is_award\":0,\"add_time\":1450319788,\"type\":{\"int\":1},\"sharecode\":\"\",\"fdis\":{\"int\":0},\"agents\":{\"string\":\"android\"}}";
        Schema schema = new Schema.Parser().parse(new File("D:\\workspace\\taotaole_statistics\\src\\main\\java\\avro\\zz_yundb.avro"));
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema,line);
        GenericDatumReader<zz_yundb> datumReader = null;
        zz_yundb key = null;
        try {
            datumReader = new GenericDatumReader<zz_yundb>(schema);
            key = datumReader.read(null, decoder);
        } catch(IOException e) {
            e.printStackTrace();
        }
        if(key == null) {
            System.err.println("Error parsing key ");
        }
    }
}
