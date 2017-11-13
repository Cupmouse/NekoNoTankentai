package net.nekonium.explorer;

import java.sql.SQLException;

public class Main {

    private static ExplorerBackend explorerBackend;

    public static void main(String[] args) {
        explorerBackend = new ExplorerBackend();

        try {
            explorerBackend.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
