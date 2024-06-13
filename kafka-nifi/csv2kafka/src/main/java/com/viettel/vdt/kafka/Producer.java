package com.viettel.vdt.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Producer {
    private static final String TOPIC_NAME = "vdt2024";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094";
    private static final String DATA_PATH = "../data/log_action.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Gson gson = new Gson();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new FileReader(DATA_PATH))) {

            String line;
            // Skip the header line if necessary
            // reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                JsonObject data = new JsonObject();
                data.addProperty("student_code", Integer.parseInt(fields[0]));
                data.addProperty("activity", fields[1]);
                data.addProperty("numberOfFile", Integer.parseInt(fields[2]));
                data.addProperty("timestamp", fields[3]);

                String dataJson = gson.toJson(data);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, dataJson);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Record sent successfully - topic: " + metadata.topic() +
                                    ", partition: " + metadata.partition() +
                                    ", offset: " + metadata.offset());
                        } else {
                            System.err.println("Error while sending record: " + exception.getMessage());
                        }
                    }
                });
            }

            producer.flush();
            System.out.println("Records sent to Kafka successfully.");
        } catch (IOException e) {
            System.out.println("Source not found or Can't connect to Kafka Broker");
            e.printStackTrace();
        }
    }
}
