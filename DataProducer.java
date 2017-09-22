package main.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class DataProducer {

    public static void main(String[] args) throws Exception {
        /*
            Task 1:
            In Task 1, you need to read the content in the tracefile we give to you, 
            and create two streams, feed the messages in the tracefile to different 
            streams based on the value of "type" field in the JSON string.

            Please note that you're working on an ec2 instance, but the streams should
            be sent to your samza cluster. Make sure you can consume the topics on the
            master node of your samza cluster before make a submission. 
        */
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.16.159:9092,172.31.19.239:9092,172.31.16.180:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        BufferedReader br = new BufferedReader(new FileReader("tracefile"));
        try {
            String line = br.readLine();
            while (line != null) {
                JSONObject jsonObject = new JSONObject(line);
                String type = jsonObject.getString("type");
                String blockId = String.valueOf(jsonObject.getInt("blockId"));
                if(type.equals("DRIVER_LOCATION")) {
                    producer.send(new ProducerRecord<String, String>("driver-locations", blockId, line));
                } else {
                    producer.send(new ProducerRecord<String, String>("events", blockId, line));
                }
                line = br.readLine();
            }

        } finally {
            br.close();
            producer.close();
        }

    }
}
