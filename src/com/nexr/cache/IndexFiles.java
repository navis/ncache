package com.nexr.cache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import static java.nio.file.StandardOpenOption.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.nexr.cache.Region.*;
import com.nexr.cache.util.LinkedSet;

public class IndexFiles {

    private static final int HEADER_LEN = 8;
    private static final int RECORD_LEN = 13;
    private static final int BULK_LOAD = RECORD_LEN << 12;
    private static final int INDEX_FILE_LEN = 128 << 20;

    private static final byte PUT = 0;
    private static final byte REMOVE = 1;

    final File dir;
    final MemoryPool pool;
    final IndexMap indexes;

    int counter;

    boolean using1;
    IndexFile file1;
    IndexFile file2;

    IndexFiles(File dir, MemoryPool pool) {
        this.dir = dir;
        this.pool = pool;
        this.using1 = true;
        this.indexes = new IndexMap();
    }

    public List<int[]> intialize(boolean compact) throws IOException {
        file1 = findFile(1);
        file2 = findFile(2);

        if (file1.stamp < 0 && file2.stamp < 0) {
            start(file1);
            return Collections.emptyList();
        }
        counter = Math.max(file1.stamp, file2.stamp);

        if (file1.stamp > file2.stamp) {
            readFile(file2, file1);
        } else {
            using1 = false;
            readFile(file1, file2);
        }
        if (compact) {
            compact();
        }
        return snapshot();
    }

    private void start(IndexFile file) throws IOException {
        file.stamp = ++counter;
        writeHeader(file);
        file.rposition(0);
    }

    private IndexFile findFile(int index) throws IOException {
        File file = new File(dir, "index." + index);
        FileChannel channel = FileChannel.open(file.toPath(), CREATE, READ, WRITE);
        IndexFile ifile = new IndexFile(channel, file.getName());

        if (channel.size() >= HEADER_LEN) {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_LEN);
            ifile.read(buffer);
            buffer.flip();
            ifile.stamp = buffer.getInt();
            ifile.start = buffer.getInt();
        } else {
            ifile.stamp = -1;
        }
        System.out.println("[IndexFiles/findFile] init " + ifile);
        return ifile;
    }

    private void readFile(IndexFile prev, IndexFile current) throws IOException {
        if (prev.length() > 0) {
            readIndex(prev, true);
            readIndex(current, false);
        } else {
            readIndex(current, true);
        }
    }

    private void readIndex(IndexFile file, boolean readAll) throws IOException {
        file.rposition(readAll ? 0 : file.start);

        int[] bindex = pool.allocate(BULK_LOAD);
        ByteBuffer region = pool.regionFor(bindex, true);
        try {
            while (file.read(region)) {
                readIndex(region);
            }
            readIndex(region);
        } finally {
            pool.release(bindex);
        }
    }

    private void readIndex(ByteBuffer region) {
        System.out.println("[IndexFiles/readIndex] " + region);
        region.flip();
        while (region.remaining() >= RECORD_LEN) {
            byte operation = region.get();
            int sindex = region.getInt();
            int offset = region.getInt();
            int length = region.getInt();

            int[] index = Region.index(sindex, offset, length);
            System.out.println("[IndexFiles/readIndex] " + operation + "::" + Region.toString(index));
            if (operation == PUT) {
                indexes.put(index);
            } else {
                indexes.remove(index);
            }
        }
        region.compact();
    }

    public synchronized int size() {
        return indexes.size();
    }

    public synchronized String dump() {
        StringBuilder builder = new StringBuilder();
        builder.append(current()).append(" --> ");
        for (int[] index : indexes) {
            builder.append(Region.toString(index)).append(' ');
        }
        return builder.toString();
    }

    public int[] put(int[] index) throws IOException {
        writeIndex(index, PUT);
        return putIndex(index);
    }

    public int[] remove(int[] index) throws IOException {
        writeIndex(index, REMOVE);
        return removeIndex(index);
    }

    private void writeIndex(int[] index, byte operation) throws IOException {
        int[] sindex = pool.allocate(RECORD_LEN);
        ByteBuffer region = pool.regionFor(sindex, true);
        region.put(operation);
        region.putInt(index[0]);
        region.putInt(index[1]);
        region.putInt(index[2]);
        try {
            current().write(region);
        } finally {
            pool.release(sindex);
        }
    }

    public void compact() throws IOException {
        System.out.println("-- [IndexFiles/compact] " + current());
        IndexFile prev = current().compacting();
        List<int[]> indexes = switchFile();
        compact(prev, current(), indexes);
    }

    private synchronized List<int[]> switchFile() throws IOException {
        List<int[]> snapshot = snapshot();

        using1 = !using1;
        IndexFile next = current();
        next.rposition(next.start = snapshot.size());
        next.nextStamp(++counter);
        writeHeader(next);
        return snapshot;
    }

    private void writeHeader(IndexFile ifile) throws IOException {
        int[] bindex = pool.allocate(HEADER_LEN);
        ByteBuffer region = pool.regionFor(bindex, true);
        try {
            region.putInt(ifile.stamp);
            region.putInt(ifile.start);
            ifile.pwrite(region, 0);
        } finally {
            pool.release(bindex);
        }
    }

    private synchronized List<int[]> snapshot() {
        List<int[]> snapshot = new ArrayList<int[]>(indexes.size());
        for (int[] index : indexes) {
            snapshot.add(index);
        }
        return snapshot;
    }

    private synchronized int[] putIndex(int[] index) {
        return indexes.put(index);
    }

    private synchronized int[] removeIndex(int[] index) {
        return indexes.remove(index);
    }

    private IndexFile check() throws IOException {
        IndexFile current = current();
        if (current.cursor + RECORD_LEN > INDEX_FILE_LEN) {
            compact();
        }
        return (using1 ? file1 : file2).await();
    }

    private synchronized IndexFile current() {
        return (using1 ? file1 : file2).await();
    }

    private void compact(IndexFile prev, IndexFile current, List<int[]> indexes) throws IOException {
        System.out.println("-- [IndexFiles/compact] " + prev.file + " --> " + current.file);
        long position = HEADER_LEN;
        int[] bindex = pool.allocate(Math.min(BULK_LOAD, indexes.size() * RECORD_LEN));
        ByteBuffer region = pool.regionFor(bindex, true);
        try {
            for (int[] index : indexes) {
                System.out.println("[IndexFiles/compact] " + Region.toString(index) + " to " + (position + region.position()));
                if (region.remaining() < RECORD_LEN) {
                    position += current.pwrite(region, position);
                    region.compact();
                }
                region.put(PUT);
                region.putInt(index[0]);
                region.putInt(index[1]);
                region.putInt(index[2]);
            }
        } finally {
            if (region.position() > 0) {
                current.pwrite(region, position);
            }
            pool.release(bindex);
            prev.compacted();
        }
    }

    private static class IndexMap extends LinkedSet<int[]> {
        
        public IndexMap() {
            super(false);
        }

        @Override
        protected int hashFor(int[] index) {
            return (index[SLAB_INDEX] << 24) + index[OFFSET_INDEX];
        }

        @Override
        protected boolean equals(int[] index1, int[] index2) {
            return index1[SLAB_INDEX] == index2[SLAB_INDEX] && index1[OFFSET_INDEX] == index2[OFFSET_INDEX] && index1[LENGTH_INDEX] == index2[LENGTH_INDEX];
        }
    }

    private static class IndexFile {

        int stamp;
        int start;
        String file;
        FileChannel channel;

        boolean compacting;
        long cursor;

        public IndexFile(FileChannel channel, String file) {
            this.channel = channel;
            this.file = file;
        }

        public void nextStamp(int next) {
            this.stamp = next;
        }

        public void rposition(int start) throws IOException {
            System.out.println("-- [IndexFile/rposition] " + this + " --> " + start);
            position(HEADER_LEN + start * RECORD_LEN);
        }

        public synchronized long length() throws IOException {
            return channel.size();
        }

        public synchronized void position(long position) throws IOException {
            channel.position(position);
            cursor = position;
        }

        public synchronized long position() throws IOException {
            return channel.position();
        }

        public synchronized int write(ByteBuffer region) throws IOException {
            region.flip();
            int written = channel.write(region);
            cursor += written;
            return written;
        }

        public synchronized int pwrite(ByteBuffer region, long position) throws IOException {
            region.flip();
            return channel.write(region, position);
        }

        public synchronized boolean read(ByteBuffer region) throws IOException {
            while (region.hasRemaining()) {
                int read = channel.read(region);
                if (read < 0) {
                    return false;
                }
            }
            return true;
        }

        public synchronized int readInt() throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_LEN);
            read(buffer);
            buffer.flip();
            return buffer.getInt();
        }

        public synchronized void truncate() {
            try {
                channel.truncate(0);
            } catch (Exception e) {
                // ignore
            }
        }

        public synchronized IndexFile await() {
            while (compacting) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return this;
        }

        public synchronized IndexFile compacting() {
            compacting = true;
            notifyAll();
            return this;
        }

        public synchronized IndexFile compacted() throws IOException {
            compacting = false;
            truncate();
            notifyAll();
            return this;
        }

        public String toString() {
            return stamp + ":" + start + "[" + file + "]";
        }
    }
}
