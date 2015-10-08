package io.confluent.urlcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CountingBufferedReader extends BufferedReader {
    private InputStreamReader reader;
    long count = 0;
    
    public CountingBufferedReader(InputStreamReader reader) {
        super(reader);
        this.reader = reader;
    }
    
    int carryon = -1;
    public String readline() throws IOException {
     StringBuilder builder = new StringBuilder();
     boolean foundEnd = false;
     if(carryon != -1) {
         builder.append(carryon);
     }
     
     while(foundEnd) {
         int c = reader.read();
         if(c == -1) {
             foundEnd = true;
             continue;
         }

         if(((char) c) == '\n') {
             count++;
             carryon = reader.read();
             if(carryon == '\r') {
                 count++;
                 carryon = -1;
             }
             foundEnd = true;
             continue;
         }
         count++;
         builder.append( (char ) c );
     }

     return builder.toString();
    }
    
    public void close() throws IOException {
        super.close();
    }
}
