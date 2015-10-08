package io.confluent.urlcount;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StoreElement {
    private long version;
    private byte[] value;
    private byte[] bytes;
    
    public StoreElement(long version, byte[] value) throws IOException {
        this.version = version;
        this.value = value;
        serialize();
    }
    
    public StoreElement(byte[] bytes) throws IOException {
        this.bytes = bytes;
        this.value = null;
        deserialize();
    }
    
    private void serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8 + 4);
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(this.version);
        dos.write(this.value);
        
        this.bytes = baos.toByteArray();
    }
    
    private void deserialize() throws IOException {
        if(this.bytes == null) {            
            return;
        }
        
        ByteArrayInputStream bais = new ByteArrayInputStream(this.bytes);
        DataInputStream dis = new DataInputStream(bais);
        
        this.version = dis.readLong();
        this.value = new byte[this.bytes.length - 8];
        dis.read(this.value);
    }
    
    public byte[] getBytes() {
        return bytes;
    }
    
    public long getVersion() {
        return this.version;
    }
    
    public byte[] getValue() {
        return value;
    }
    
    public void set(long version, byte[] value) throws IOException {
        this.version = version;
        this.value = value;
        serialize();
    }
}
