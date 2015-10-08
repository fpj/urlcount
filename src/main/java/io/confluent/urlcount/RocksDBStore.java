package io.confluent.urlcount;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key-value store backed by RocksDB.
 *
 */
public class RocksDBStore implements KVStore {
    public static Logger logger = LoggerFactory.getLogger(RocksDBStore.class);
    
    Properties properties;
    RocksDB db;
    
    public RocksDBStore(Properties properties) {
        this.properties = properties;
    }

    public void init() throws IOException {
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        Options options = new Options().setCreateIfMissing(true);
        db = null;
        try {
          // a factory method that returns a RocksDB instance
          logger.info("Path to db is: {}", properties.getProperty("rocksdb.pathToDb"));
          db = RocksDB.open(options, properties.getProperty("rocksdb.pathToDb"));
        } catch (RocksDBException e) {
          // do some error handling
          throw new IOException(e);
        }

    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

    }

    public void put( byte[] key, byte[] value, long version)
            throws IOException {
        try {
            byte[] elementBytes = db.get(key);
            if(elementBytes != null) {
                StoreElement element = new StoreElement(elementBytes);
                if(version > element.getVersion()) {
                    element.set(version, value);
                    db.put(key, element.getBytes());
                } else {
                    logger.warn("Version check doesn't pass: {}, {}", version, element.getVersion());
                    throw KVStoreException.create(KVStoreException.Code.VERSION_MISMATCH);
                }
            } else {
                StoreElement element = new StoreElement(version, value);
                db.put(key, element.getBytes());
            } 
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }
    
    public byte[] get(byte[] key) throws IOException {
        byte[] value = null; 
        ByteBuffer bb = null;
        try {
            byte[] bytes = db.get(key);
            if(bytes != null) {
                bb = ByteBuffer.wrap(bytes);
                value = new byte[bb.capacity()];
                bb.get(value);
                logger.debug("Buffer capacity: " + bb.capacity());
            } 
        } catch (RocksDBException e) {
            throw new IOException(e);
        } catch (NegativeArraySizeException e) {
            logger.error("Buffer capacity: " + bb.capacity());
        }
        
        return value;
    }
}
