package io.confluent.urlcount;

import io.confluent.urlcount.KVStoreException.VersionMismatchException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlCountConsumer implements Runnable {
    public static Logger logger = LoggerFactory.getLogger(UrlCountConsumer.class);

    
    boolean isRunning = true;
    Properties properties;
   
    KafkaConsumer<String, byte[]> consumer;
    KVStore db;
    
    UrlCountConsumer(Properties properties) {
        this.properties = properties;
        this.db = new RocksDBStore(properties);
    }
    
    public void init() throws IOException {
        this.db.init();
    }
        
    public void run() {
        ByteBuffer bb;
        System.out.println("Initializing consumer");     
        this.consumer = new KafkaConsumer<String, byte[]>(properties);
        System.out.println("Subscribing");
        ArrayList<String> topicList = new ArrayList<String>();
        TopicPartition tp = null;
        long offset = -1;
        
        topicList.add(properties.getProperty("urlcount.topic"));
        consumer.subscribe(topicList);
        while(isRunning) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            if(records != null) {
                logger.info("Records count: {}", records.count());
                try {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String msg = record.key();
                        logger.debug("Message: " + msg + ", offset " + record.offset());
                        bb = ByteBuffer.wrap(record.value());
                        long version = bb.getLong();
                        int count = bb.getInt();
                        byte[] v = db.get(msg.getBytes());
                        
                        if(v != null) {
                            logger.info("Value length: {}", v.length);
                            bb = ByteBuffer.wrap(v);
                            bb.getLong();
                            count += bb.getInt();
                        }
                        
                        bb = ByteBuffer.allocate(4);
                        bb.putInt(count);
                        try{
                            db.put(record.key().getBytes(), bb.array(), version);
                        } catch (VersionMismatchException e) {
                            // Nothing to do
                        }
                        
                        tp = new TopicPartition(record.topic(), record.partition());
                        offset = record.offset();
                        if(offset % 50 == 0) {
                            consumer.commitAsync();
                        }
                    }
                } catch (IOException e) {
                    logger.warn("Got an exception out of the KVStore: {}", e );
                    e.printStackTrace();
                    if(tp != null) {
                        consumer.seek(tp, offset + 1);
                    }
                } catch ( Exception e ) {
                    e.printStackTrace();
                }
                
                logger.debug("Another consumer iteration");
                
            } 
        }
        consumer.commitSync();
        consumer.close();   
        logger.info("Shutting down Thread: " + Thread.currentThread().getId());
    }
    
    public KVStore getDB() {
        return db;
    }
    
    public void shutdown() {
        this.isRunning = false;
    }
}
