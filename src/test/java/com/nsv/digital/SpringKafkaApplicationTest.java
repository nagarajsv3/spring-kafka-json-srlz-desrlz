package com.nsv.digital;

import com.nsv.digital.kafka.consumer.Receiver;
import com.nsv.digital.kafka.domain.Car;
import com.nsv.digital.kafka.producer.Sender;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Java6Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaApplicationTest {

    @Value("${kafka.topic.jsontopic}")
    private String C3TOPIC = null;
    //private static final String C3TOPIC = "c3testtopic";

    //Uncomment to use embedded kafka broker
    /*@ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1,true,C3TOPIC);*/

    @Autowired
    private Receiver receiver;

    @Autowired
    private Sender sender;

    @Test
    public void testReceive() throws Exception {
        //sender.send(C3TOPIC,"Hey Kafka Broker: Naga Here");

        Car car = new Car("Camry", "Toyato", "DEF-123");
        sender.send(car);

        receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(receiver.getLatch().getCount()).isEqualTo(0);
    }

}