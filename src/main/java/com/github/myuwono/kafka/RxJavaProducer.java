package com.github.myuwono.kafka;

import io.reactivex.Observable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class RxJavaProducer {

    private static final String KAFKA_BROKERS_ENV = "KAFKA_BROKERS";

    private static final String TOPIC_ENV = "INPUT_TOPIC";

    private static final String NUMBER_OF_REPEATS_ENV = "NUMBER_OF_REPEATS";

    private static final String TOPIC = Optional.ofNullable(System.getenv(TOPIC_ENV))
            .orElseThrow(() -> new IllegalArgumentException(TOPIC_ENV + " is not set"));

    private static final String KAFKA_BROKERS = Optional.ofNullable(System.getenv(KAFKA_BROKERS_ENV))
            .orElseThrow(() -> new IllegalArgumentException(KAFKA_BROKERS_ENV + " is not set"));

    private static final String NUMBER_OF_REPEATS = Optional.ofNullable(System.getenv(NUMBER_OF_REPEATS_ENV))
            .orElseThrow(() -> new IllegalArgumentException(NUMBER_OF_REPEATS_ENV + " is not set"));

    private static final Producer<String, String> PRODUCER = initProducer();

    private static final Logger log = LoggerFactory.getLogger(RxJavaProducer.class);

    public static void main (String[] args) {
        Observable<String> just = Observable.just("w1", "w2");
        Observable.just(true).repeat(Long.valueOf(NUMBER_OF_REPEATS))
                .flatMap(v -> just)
                .buffer(10)
                .map(words -> String.join(" ", words))
                .map(line -> {
                    log.info("sending {}", line);
                    return line;
                })
                .subscribe(RxJavaProducer::sendToKafka);

        PRODUCER.close();
    }

    static Producer<String, String> initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BROKERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", Serdes.String().serializer().getClass());
        props.put("value.serializer", Serdes.String().serializer().getClass());
        return new KafkaProducer<>(props);
    }

    static void sendToKafka(String message) {
        PRODUCER.send(new ProducerRecord<>(TOPIC, message));
    }
}
