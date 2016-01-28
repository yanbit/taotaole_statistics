package function;

import backtype.storm.tuple.Values;
import com.google.common.collect.Iterators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import taotaole.avro.zz_yundb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Map;

/**
 * User: yanbit
 * Date: 2016/1/18
 * Time: 10:51
 */
public class YundbFunction extends BaseFunction {
    private static Log LOG = LogFactory.getLog(GoodsFunction.class);

    private GenericDatumReader<zz_yundb> datumReader = null;
    private Schema schema;
    private SpecificDatumReader<zz_yundb> din;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        try {
            schema = new Schema.Parser().parse(new File("src/main/java/avro/zz_yundb.avro"));
            SpecificDatumReader<zz_yundb> din = new SpecificDatumReader<zz_yundb>(zz_yundb.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    int count = 0;
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        JsonDecoder decoder = null;
        try {
            System.out.println(new String(tuple.getByteByField("str"),"utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            decoder = DecoderFactory.get().jsonDecoder(schema, tuple.getString(0));
            zz_yundb yundb = din.read(null, decoder);
            collector.emit(new Values(yundb.getId(),yundb.getOrderId(),yundb.getMid(),yundb.getUsername(),yundb.getBuyId(),yundb.getGoodsName(),
                    new BigInteger(yundb.getGoodsPrice().array()),new BigInteger(yundb.getPrice().array()),yundb.getQty(),new BigInteger(yundb.getTotal().array()),
                    yundb.getIp(),yundb.getIsShow(),yundb.getIsAward(),yundb.getAddTime(), Iterators.getLast(yundb.getType().values().iterator()),yundb.getSharecode(),
                    yundb.getIp(),yundb.getIsShow(),yundb.getIsAward(),yundb.getAddTime(),Iterators.getLast(yundb.getType().values().iterator()),yundb.getSharecode(),
                    Iterators.getLast(yundb.getFdis().values().iterator()), Iterators.getLast(yundb.getAgents().values().iterator())));
        } catch (IOException e) {
            count++;
            e.printStackTrace();
            Runtime.getRuntime().exit(0);
            //System.out.println("##########################"+tuple.getString(0));
            //e.printStackTrace();
        }

    }

}
