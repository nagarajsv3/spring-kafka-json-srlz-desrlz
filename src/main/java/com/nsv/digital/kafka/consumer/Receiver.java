package com.nsv.digital.kafka.consumer;

import com.nsv.digital.kafka.domain.Car;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class Receiver {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.jsontopic}")
    public void receive(Car car) {
        LOGGER.info("received payload='{}'", car);
        latch.countDown();
    }

}
