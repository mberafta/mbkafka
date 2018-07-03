package mbkafka;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    private static Scanner in;

    public static void main(String[] argv) throws Exception {
        Properties props = new Properties();
        String topicName = argv[0];

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        prepareAvro();

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(props);
        
        in = new Scanner(System.in);

        String line = in.nextLine();
        while(!line.equals("exit")){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, line);
            producer.send(record);
            line = in.nextLine();
        }

        in.close();
        producer.close();
    }

    private static void prepareAvro() throws Exception{
        String filePath = System.getProperty("user.home") + "/mbtest.avsc";
        Schema schema = new Schema.Parser().parse(new File(filePath));
        
        GenericRecord mbtest = new GenericData.Record(schema);
        mbtest.put("id","123456");
        mbtest.put("name","mbtest");

        File avroFile = new File(System.getProperty("user.home") + "/mbtest.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, avroFile);
		dataFileWriter.append(mbtest);
		dataFileWriter.close();
    }
}