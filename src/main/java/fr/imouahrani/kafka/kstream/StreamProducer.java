package fr.imouahrani.kafka.kstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StreamProducer {
    private int counter;
    private String KAFKA_BROKER_URL="localhost:9092";
    private String TOPIC_NAME = "testTopic";
    private String clientID="client_prod_1";
    private String message;

    public static void main(String[] args){
        new StreamProducer();
    }
    public StreamProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER_URL);
        properties.put("client.id", clientID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        Random random = new Random();
        List<Character> characters = new ArrayList<>();
        for(char i='A'; i<'Z'; i++){
            characters.add(i);
        }
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
            ++counter;
            message="";
            for (int i = 0; i<10; i++){
                 message +=" "+characters.get(random.nextInt(characters.size()));
            }
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, ""+(++counter), message),
                    (metadata,ex)->{
                        System.out.println("Sending message key=>"+counter+"Value =>"+message);
                        System.out.println("Partition =>"+metadata.partition()+" Offset=>"+metadata.offset());
                    });
                },1000,1000, TimeUnit.MILLISECONDS);
    }
}
