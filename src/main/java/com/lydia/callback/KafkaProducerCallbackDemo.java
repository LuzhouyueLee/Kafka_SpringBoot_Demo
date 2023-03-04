package com.lydia.callback;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@Slf4j
public class KafkaProducerCallbackDemo {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerCallbackDemo.class, args);
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    /**
     * 生产者Callback1
     */
    @GetMapping("/send/callback1/{input}")
    public void sendWithCallbackOne(@PathVariable String input) {
        //生产者，把收到的消息用KafkaTemplate发出去
        log.info("从Rest服务接收到内容：{}， 将发往topic_hello...", input);
        //发到topic_hello上
        this.kafkaTemplate.send("topic_hello", input).addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {
				log.error("发送失败,{}", throwable);
                log.error("发送失败,cause={}", throwable.getCause());
            }

            @Override
            public void onSuccess(SendResult<Object, Object> objectObjectSendResult) {
				log.info("Callback1发送成功:{}", objectObjectSendResult.toString());
            }
        });
    }

    /**
     * 生产者Callback2
     */
    @GetMapping("/send/callback2/{input}")
    public void sendWithCallbackTwo(@PathVariable String input) {
        //生产者，把收到的消息用KafkaTemplate发出去
        log.info("从Rest服务接收到内容：{}， 将发往topic_hello...", input);
        //发到topic_hello上
        ListenableFuture<SendResult<Object,Object>> future = this.kafkaTemplate.send("topic_hello", input);
        try {
            SendResult<Object,Object> result = future.get();
            log.info("Callback2发送成功:{}", result.toString());
        }catch (Throwable throwable){
            log.error("发送失败,{}", throwable);
            log.error("发送失败,cause={}", throwable.getCause());
        }
    }

}
