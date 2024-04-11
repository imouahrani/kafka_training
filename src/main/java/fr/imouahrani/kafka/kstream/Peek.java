package fr.imouahrani.kafka.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Peek {
    public static void main(String[] args) throws FileNotFoundException {
        Properties props = new Properties();

        try(FileInputStream fis = new FileInputStream("src/main/resources/config/kstream.properties")){
            props.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        final String INPUT_TOPIC = props.getProperty("input.topic");
        final String OUTPUT_TOPIC = props.getProperty("output.topic");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        source.
                peek((k,v) -> {
                    System.out.printf("key %s : value %s \n", k, v);
                });

        source.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams kafkastreams = new KafkaStreams(builder.build(), props);
        kafkastreams.start();

    }
}