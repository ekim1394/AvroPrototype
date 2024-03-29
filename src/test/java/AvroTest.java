import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by simpl on 5/13/2017.
 */
public class AvroTest {
    private final static Logger logger = LoggerFactory.getLogger(AvroTest.class);

    private Schema schema;
    private GenericRecord user1;

    @Before
    public void generateData() {
        ClassLoader classLoader = getClass().getClassLoader();

        try {
            schema = new Schema.Parser().parse(new File(classLoader.getResource("users.avsc").getFile()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Creating users");

        user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        logger.info(user1.toString());
    }

    @Before
    public void setKafka() {
        logger.info("Setting producer");
        Producer producer = new KafkaAvroClient().setProducer();

        logger.info("Setting Consumer");
        Consumer consumer = new KafkaAvroClient().setConsumer();
    }

    @Test
    public void serializeDataTest() throws IOException {
        File file = new File("src/test/resources/users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.close();
    }

    @Test
    public void deserializeDataTest() throws IOException {
        File file = new File("src/test/resources/users.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            logger.info(user.toString());
        }
    }

    @Test
    public void sendSerializedtoTopicTest() {

    }
}
