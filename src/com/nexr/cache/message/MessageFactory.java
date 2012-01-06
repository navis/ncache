package com.nexr.cache.message;

public class MessageFactory {

    public static Message createServer(byte opcode) {
        switch (opcode) {
            case Message.EOF:
                return new EOF();
            case Message.ACK:
                return new Ack();
            case Message.NACK:
                return new Nack();
            case Message.REQ_GET:
                return new Get();
            case Message.REQ_STORE:
                return new ClientPushed();
            case Message.REQ_REMOVE:
                return new Remove();
            case Message.REQ_FLUSH:
                return new Flush();
            case Message.FWD_STORE:
                return new StoreForwarded();
            case Message.FWD_REMOVE:
                return new RemoveForwarded();
            case Message.REPORT:
                return new StatusReport();
            case Message.SNAPSHOT:
                return new Snapshot();
            case Message.SYNC_REQS:
                return new SyncRequests();
            case Message.SYNC_DATA:
                return new SyncPushed();
            case Message.SYNC_COMPLETED:
                return new SyncCompleted();
        }
        throw new IllegalStateException("illegal opcode = " + opcode);
    }

    public static Message createClient(byte opcode) {
        switch (opcode) {
            case Message.ACK:
                return new Ack();
            case Message.NACK:
                return new Nack();
            case Message.RETRIEVED:
                return new ClientRetrieve();
        }
        throw new IllegalStateException("illegal opcode = " + opcode);
    }

    public static String toString(byte opcode) {
        switch (opcode) {
            case Message.EOF:
                return "EOF";
            case Message.ACK:
                return "ACK";
            case Message.NACK:
                return "NACK";
            case Message.REQ_GET:
                return "REQ_GET";
            case Message.REQ_STORE:
                return "REQ_STORE";
            case Message.REQ_REMOVE:
                return "REQ_REMOVE";
            case Message.REQ_FLUSH:
                return "REQ_FLUSH";
            case Message.FWD_STORE:
                return "FWD_STORE";
            case Message.FWD_REMOVE:
                return "FWD_REMOVE";
            case Message.RETRIEVED:
                return "RETRIEVE";
            case Message.REPORT:
                return "REPORT";
            case Message.SNAPSHOT:
                return "SNAPSHOT";
            case Message.SYNC_REQS:
                return "SYNC_REQ";
            case Message.SYNC_DATA:
                return "SYNC_DATA";
            case Message.SYNC_COMPLETED:
                return "SYNC_COMPLETED";
        }
        return "illegal opcode = " + opcode;
    }
}
