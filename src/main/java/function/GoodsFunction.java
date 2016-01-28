package function;

import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: yanbit
 * Date: 2016/1/15
 * Time: 16:05
 */
public class GoodsFunction extends BaseFunction {
    private static Log LOG = LogFactory.getLog(GoodsFunction.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String[] lines = tuple.getString(0).split(",");
        if (3 == lines.length) {
            collector.emit(new Values(Integer.valueOf(lines[0]), lines[1], lines[2]));
        } else {
            LOG.error("##### Goods format error message:" + tuple.getString(0));
        }
    }

}
