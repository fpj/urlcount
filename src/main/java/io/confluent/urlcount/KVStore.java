package io.confluent.urlcount;

import java.io.IOException;

public interface KVStore {
    void init() throws IOException;
    void put(byte[] key, byte[] value, long version) throws IOException;
    byte[] get(byte[] key) throws IOException;
}
