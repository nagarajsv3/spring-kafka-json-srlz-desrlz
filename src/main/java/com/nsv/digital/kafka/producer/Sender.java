package com.nsv.digital.kafka.producer;

import com.nsv.digital.kafka.domain.Car;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Sender.class);

    @Value("${kafka.topic.jsontopic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, Car> kafkaTemplate;

    public void send(Car car) {
        LOGGER.info("sending payload='{}' to topic='{}'", car.toString(), topic);
        kafkaTemplate.send(topic, car);
    }
}