package com.lydia;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.LinkedList;

@SpringBootApplication
@RestController
@Slf4j
public class CreateTopicDemo {
    @Autowired
    private KafkaProperties properties;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    public static void main(String[] args) {
        SpringApplication.run(CreateTopicDemo.class, args);
    }

    @GetMapping("/create/{topic}/{numPartitions}")
    public void createTopic(@PathVariable String topic, @PathVariable int numPartitions) {
        AdminClient client = AdminClient.create(properties.buildAdminProperties());
        if(client !=null){
            try {
                Collection<NewTopic> topics = new LinkedList<>();
                short replications = 2;
                log.info("Receives topic value={}, numPartition={}", topic, numPartitions);
                //创建topic，含有topic名称，分区数，副本数
                topics.add(new NewTopic(topic ,numPartitions,replications));
                client.createTopics(topics);
            }catch (Throwable e){
                e.printStackTrace();
                log.error("Create topic occurs with error {}", e.getCause());
            }finally {
                client.close();
            }
        }
    }
}
