package io.confluent.urlcount;

import java.util.Arrays;

public final class ByteArrayKey {
    byte[] data;
    
    public ByteArrayKey(byte[] data) {
        this.data = data; 
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayKey))
        {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayKey) other).data);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }
}
