package com.nexr.cache;

public class FullName {

    String cname;
    String sname;

    public FullName(String cname, String sname) {
        this.cname = cname;
        this.sname = sname;
    }

    public FullName(String fullname) {
        int index = fullname.indexOf(':');
        this.cname = fullname.substring(0, index);
        this.sname = fullname.substring(index + 1);
    }

    public String cluster() {
        return cname;
    }

    public String server() {
        return sname;
    }

    public boolean isSameServer(String name) {
        return name.equals(sname);
    }

    public String toString() {
        return cname + ":" + sname;
    }
}
