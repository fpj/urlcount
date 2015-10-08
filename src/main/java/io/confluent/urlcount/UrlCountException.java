package io.confluent.urlcount;

@SuppressWarnings( "serial" )
public class UrlCountException extends Exception {
    enum Code {
        VERSION_MISMATCH,
        SYSTEM_FAILURE
    }
    
    public UrlCountException(String message) {
        super(message);
    }
    
    public static UrlCountException create(Code c) {
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
    
    public static class VersionMismatchException extends UrlCountException {
        public VersionMismatchException() {
            super(getMessage(Code.VERSION_MISMATCH));
        }
    }
    
    public static class SystemFailureException extends UrlCountException {
        public SystemFailureException() {
            super(getMessage(Code.SYSTEM_FAILURE));
        }
    }
}
