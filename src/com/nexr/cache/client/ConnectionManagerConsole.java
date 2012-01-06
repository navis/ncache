package com.nexr.cache.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class ConnectionManagerConsole {

    public static void main(String[] args) throws Exception {
        ServiceHandler manager = new ServiceHandler(args[0], args[1]);
        manager.initialize();
        try {
            while (handleCommand(manager)) {
            }
        } finally {
            manager.shutdown();
        }
    }

    private static boolean handleCommand(ServiceHandler manager) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = reader.readLine()) != null) {
            String[] commands = line.split("\\s");
            if (commands[0].equals("put")) {
                boolean result;
                if (commands.length == 3) {
                    result = manager.put(commands[1], commands[2].getBytes());
                } else if (commands.length == 4) {
                    result = manager.put(commands[1], commands[2].getBytes());
                } else {
                    System.out.println("invalid command " + line);
                    continue;
                }
                System.out.println(" return : " + result);
            } else if (commands[0].equals("get")) {
                byte[] value = manager.get(commands[1]);
                System.out.println(" return : " + (value == null ? "null" : new String(value)));
            } else if (commands[0].equals("remove")) {
                boolean result = manager.remove(commands[1]);
                System.out.println(" return : " + result);
            }
        }
        return false;
    }
}
