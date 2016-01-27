package function;

import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * User: yanbit
 * Date: 2016/1/18
 * Time: 10:51
 */
public class GoodsFunction2 extends BaseFunction {
        private static Log LOG = LogFactory.getLog(GoodsFunction.class);

        private List fields = null;
        private String errorMessage = null;
        private String defaultSeparator = ",";

        public GoodsFunction2(List fields, String errorMessage) {
            this.fields = fields;
            this.errorMessage = errorMessage;
        }

        public GoodsFunction2(List fields, String errorMessage ,String separator){
            this(fields,errorMessage);
            this.defaultSeparator=separator;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String[] lines = tuple.getString(0).split(defaultSeparator);
            if (3 == lines.length) {
                collector.emit(new Values(Integer.valueOf(lines[0]), lines[1], lines[2]));
            } else {
                LOG.error("##### Goods format error message:" + tuple.getString(0));
            }
        }

}
