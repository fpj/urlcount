package io.confluent.urlcount;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlCountProducer {
    public static Logger logger = LoggerFactory.getLogger(UrlCountProducer.class);
    
    String topic;
    KafkaProducer<String, byte[]> producer;
    BlockingQueue<Future<RecordMetadata>> queue;
    
    UrlCountProducer(Properties properties) {
        this.topic = properties.getProperty("urlcount.topic");
        this.producer = new KafkaProducer<String, byte[]>(properties);
        this.queue = new LinkedBlockingQueue<Future<RecordMetadata>>();
    }
    
    void submit(String url, int count, long offset) throws IOException {
        UrlCount uc = new UrlCount(offset, count);
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, url, uc.getBytes());
        Future<RecordMetadata> md = producer.send(record);
        queue.add(md);
        logger.info("Producing: {}, {}", url, offset);
    }
}
