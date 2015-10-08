package io.confluent.urlcount;

import java.io.IOException;

@SuppressWarnings( "serial" )
public class KVStoreException extends IOException {
    enum Code {
        VERSION_MISMATCH,
        SYSTEM_FAILURE
    }
    
    public KVStoreException(String message) {
        super(message);
    }
    
    public static KVStoreException create(Code c) {
        switch(c) {
        case VERSION_MISMATCH:
            return new VersionMismatchException();
        default:
            return new SystemFailureException();
        }
    }

    public static String getMessage(Code c) {
        switch(c) {
        case VERSION_MISMATCH:
            return "Version mismatch";
        default:
            return "System failure";
        }
    }
    
    public static class VersionMismatchException extends KVStoreException {
        public VersionMismatchException() {
            super(getMessage(Code.VERSION_MISMATCH));
        }
    }
    
    public static class SystemFailureException extends KVStoreException {
        public SystemFailureException() {
            super(getMessage(Code.SYSTEM_FAILURE));
        }
    }
}
