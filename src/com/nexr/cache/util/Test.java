package com.nexr.cache.util;

import java.io.*;

public class Test {

    public static void main(String[] args) throws Exception {
        File files = new File("C:\\Documents and Settings\\navis\\My Documents\\Downloads\\gsod_2009\\gsod_1990");

        PrintWriter writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream("C:\\Documents and Settings\\navis\\My Documents\\Downloads\\gsod_2009\\gsod_1990\\gsod_1990")));
        for (File file : files.listFiles()) {
            if (!file.getName().endsWith(".op")) {
                continue;
            }
            System.out.println("[Test/main] " + file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file))));
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                writer.println(line);
            }
            reader.close();
        }
        writer.close();
    }
}
