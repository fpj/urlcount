package io.confluent.urlcount;

import io.confluent.urlcount.UrlCountException.Code;

import java.io.IOException;
import java.util.HashMap;


/**
 * Simple in-memory key-value store
 *
 */
public class InMemoryKVStore implements KVStore {
    HashMap<ByteArrayKey, byte[]> map;
    
    InMemoryKVStore() {
        this.map = new HashMap<ByteArrayKey, byte[]>(1000);
    }
    
    public void init() throws IOException {
        try {
            
        } catch (Exception e) { 
            throw new IOException();
        }
    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            map.put(new ByteArrayKey(key), value);
            
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    public void put(byte[] key, byte[] value, long version)
    throws IOException {
        ByteArrayKey bak = new ByteArrayKey(key); 
        try {
            StoreElement element = null;
            if(map.containsKey(bak)) {
                element = new StoreElement(map.get(key));
                if(version > element.getVersion()) {
                    element.set(version, value);
                    map.put(bak, element.getBytes());
                } else {
                    throw UrlCountException.create(Code.VERSION_MISMATCH);
                }
            } else {
                element = new StoreElement(version, value);
                map.put(bak, element.getBytes());
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public byte[] get(byte[] key) throws IOException {
        StoreElement element = new StoreElement(map.get(key));
        
        return element.getValue();
    }
}
