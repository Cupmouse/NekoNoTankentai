package net.nekonium.explorer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseManager {

    private HikariDataSource dataSource;

    public void init() throws SQLException {
        final HikariConfig config = new HikariConfig();

        config.setJdbcUrl("jdbc:mysql://localhost:3306/nek_blockchain");
        config.setUsername("root");
        config.setPassword("");

        this.dataSource = new HikariDataSource(config);
        // 接続してみる
        this.dataSource.getConnection();
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
