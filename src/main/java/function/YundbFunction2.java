package function;

import backtype.storm.tuple.TupleImpl;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * User: yanbit
 * Date: 2016/1/18
 * Time: 10:51
 */
public class YundbFunction2 extends BaseFunction {
    private static Log LOG = LogFactory.getLog(GoodsFunction.class);


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        ((TupleImpl)tuple).get("bytes");
        Message message = new Message((byte[])((TupleImpl) tuple).get("bytes"));


        ByteBuffer bb = message.payload();

        byte[] b = new byte[bb.remaining()];
        bb.get(b, 0, b.length);

        try {

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(new File("src/main/java/avro/zz_yundb.avro")));
            Decoder decoder = DecoderFactory.get().binaryDecoder(b, null);
            GenericRecord result = reader.read(null, decoder);
            System.out.println("siteId: "+ result.get("id"));
//            System.out.println("eventType: "+ result.get("eventType"));
//            Format formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//            String s = formatter.format((Long) result.get("timeStamp"));
//            System.out.println("timeStamp: " + s);
//            System.out.println("comment: " +result.get("comment") );

//            System.out.println("PLine Text: " + ((GenericRecord) result.get("subrecord")).get("text"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
