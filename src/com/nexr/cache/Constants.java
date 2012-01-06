package com.nexr.cache;

public interface Constants {

    byte[] NEWLINE = "\r\n".getBytes();

    byte ASPACE = ' ';
    byte[] SPACE = " ".getBytes();
    byte[] VALUE = "VALUE".getBytes();
    byte[] VALUE_ = "VALUE ".getBytes();
    byte[] END = "END".getBytes();
    byte[] END_N = "END\r\n".getBytes();
    byte[] STORED = "STORED".getBytes();
    byte[] STORED_N = "STORED\r\n".getBytes();
    byte[] NOT_STORED_N = "NOT_STORED\r\n".getBytes();
    byte[] OK = "OK\r\n".getBytes();

    byte[] GET = "get".getBytes();
    byte[] GET_ = "get ".getBytes();
    byte[] GETS = "gets".getBytes();
    byte[] SET = "set".getBytes();
    byte[] SET_ = "set ".getBytes();
    byte[] FLUSH_ALL = "flush_all".getBytes();


    String NCACHE_ADDRESS = "ADMIN";

    long INITIAL_SYNC_TIMEOUT = 10000;

    int E30DAYS = 30 * 24 * 60 * 60;
    
    int INFO = 0;
    int MEMORY = 1;
    int PERSIST = 2;

    int INFO_GSHIGH = 0;
    int INFO_GSLOW = 1;
    int INFO_HASH = 2;
}
