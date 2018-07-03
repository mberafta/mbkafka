package mbkafka;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
    private static Scanner in;
    private static boolean stop = false;

    public static void main(String[] argv) throws Exception, IOException {
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n", Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            // Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            // Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        // File file = new File(System.getProperty("user.home") + "/output.json");
                        // FileWriter fileWriter = new FileWriter(file, true);

                        // String recordValue = record.value();
                        // String[] parts = recordValue.split(",");
                        // JSONObject json = new JSONObject();
                        // json.put("id", parts[0]);
                        // json.put("name", parts[1]);

                        // fileWriter.append(json.toString() + "\n");
                        // System.out.println("Nouveau message -> " + json.toString());
                        // System.out.println(record.value());
                        // fileWriter.close();

                        Schema schema = new Schema.Parser()
                                .parse(new File(System.getProperty("user.home") + "/mbtest.avsc"));

                        GenericRecord emp = new GenericData.Record(schema);

                        File file = new File(System.getProperty("user.home") + "/mbtest.avro");

                        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
                        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);

                        while (dataFileReader.hasNext()) {
                            emp = dataFileReader.next();
                            System.out.println(emp);
                            dataFileReader.close();
                        }
                    }
                }
            } catch (Exception ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }
}