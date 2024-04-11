package fr.imouahrani.kafka.kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.*;


public class Basique {

    public static final String INPUT_TOPIC = "testTopic";
    public static final String OUTPUT_TOPIC = "wcTopic";

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        KTable<String, Long> wordCounts=source
                .flatMapValues(textLine-> Arrays.asList(textLine.split("w")))
                .groupBy((k,word)->word)
                .count(Materialized.as("count-store"));

        // source = source.filter((k,v)->{return v.length()> 5 ? true : false;});

       // source = source.mapValues( v -> {return v.toUpperCase();});

      /*  source = source.flatMap((k,v) -> {
            // eclatement de notre phrase en fonction du caractere espace
            String[] tokens =  v.split(" ");
            // on construit une liste pour notre valeur de retour
            List<KeyValue<String, String>> result = new ArrayList<>(tokens.length);
            for (String token : tokens) {
                // en valeur un mot de la phrase qu'on a split
                result.add(new KeyValue<>(k,token));
            }
            return  result;
        });*/

       /* source.foreach((k,v) -> {
            System.out.println(v);
        });

        */

        wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }
}
