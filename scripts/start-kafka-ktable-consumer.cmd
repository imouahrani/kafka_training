%KAFKA_HOME%\kafka-console-consumer.bat
--bootstrap-server localhost:9092 \
--topic string-output
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
