package io.confluent.urlcount;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class UrlCount {
    long version;
    int count;
    byte[] bytes;
    
    public UrlCount (long version, int count) throws IOException {
        this.version = version;
        this.count = count;
        serialize();
    }
    
    public UrlCount(byte[] bytes) throws IOException {
        this.bytes = bytes;
        deserialize();
    }
    
    private void serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8 + 4);
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(this.version);
        dos.writeInt(this.count);
        
        this.bytes = baos.toByteArray();
    }
    
    private void deserialize() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(this.bytes);
        DataInputStream dis = new DataInputStream(bais);
        
        this.version = dis.readLong();
        this.count = dis.readInt();
    }
    
    public byte[] getBytes() {
        return bytes;
    }
}
