package com.lydia.replaying;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@Slf4j
public class ReplyingKafkaTemplateDemo {

    public static void main(String[] args) {
        SpringApplication.run(ReplyingKafkaTemplateDemo.class, args);
    }


    @Autowired
    private ReplyingKafkaTemplate replyingTemplate;

    @GetMapping("/send/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendWithReplayFuture(@PathVariable String input) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic-testReplayTemplate", input);
        RequestReplyFuture<String, String, String> replyFuture = replyingTemplate.sendAndReceive(record);
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        log.info("获取消费者反馈: " + consumerRecord.value());
    }

    @KafkaListener(id = "webGroup", topics = "topic-testReplayTemplate")
    @SendTo
    public String consumer(String input) {
        log.info("消费者收到消息: {}", input);
        return "successful";
    }
}