package net.nekonium.explorer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseManager {

    private HikariDataSource dataSource;

    public void init(String url, String user, String password) throws SQLException {
        final HikariConfig config = new HikariConfig();

        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);

        this.dataSource = new HikariDataSource(config);
        // 接続してみる
        this.dataSource.getConnection();
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
