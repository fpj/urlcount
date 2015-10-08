package io.confluent.urlcount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Produces messages to be published to a Kafka topic.
 *
 */
public class UrlCountSource {
    public static Logger logger = LoggerFactory.getLogger(UrlCountSource.class);
    
    enum Type {FILE, SIMPLE};
    UrlCountProducer producer;
    File urlLogFile;
    Type type;
    
    
    UrlCountSource(Properties properties) {
        this.producer = new UrlCountProducer(properties);
        this.urlLogFile = new File(properties.getProperty("urlcount.urlLogFile"));
        String typeValue = properties.getProperty("urlcount.type");
        if(typeValue == null) {
            this.type = Type.SIMPLE;
        } else {
            this.type = Type.valueOf(typeValue);
        }
    }
    
    public void loadFile() throws IOException {
        long offset = 0;
        FileInputStream input = new FileInputStream(urlLogFile);
        CountingBufferedReader br =
                new CountingBufferedReader( new InputStreamReader(input));
        String url;
        
        while(( url = br.readLine()) != null ) {
            logger.info("Producing: {}", url);
            producer.submit(url, 1, offset);
            offset += url.getBytes().length + 1;
        }
        System.out.println("Closing");
        br.close();
        input.close();
    }
    
    public void pushLines() throws IOException {
        producer.submit("www.bla.edu", 1, 0);
        producer.submit("www.bla.io", 1, 13);
        producer.submit("www.bla.com", 1, 26);
        
        System.out.println("Closing");
    }
    
    public static void verifyDB(KVStore db) {
        
        
    }
    
    public static void main(String[] args) {
        // Need the following properties:
        // zookeeper.connect
        // group.id
        // zookeeper.session.timeout.ms
        // zookeeper.sync.time.ms
        // auto.commit.interval.ms
        // rocksdb.pathToDb
        // urlcount.topic
        Properties properties = System.getProperties();
        
        final UrlCountSource source = new UrlCountSource(properties);
        final UrlCountConsumer consumer = new UrlCountConsumer(properties);
        try{
            consumer.init();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        Thread sourceThread = new Thread() {
          @Override
          public void run() {
              try {
                  switch(source.type) {
                  case FILE:
                      source.loadFile();
                      break;
                  case SIMPLE:
                      source.pushLines();
                      break;
                  default:
                  }
            } catch (IOException e) {
                e.printStackTrace();
            }
          }
        };
        
        sourceThread.start();
        
        Thread consumerThread = new Thread(consumer);  
        consumerThread.start();
        try {
            sourceThread.join();
            consumer.shutdown();
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        verifyDB(consumer.getDB());
    }
}
