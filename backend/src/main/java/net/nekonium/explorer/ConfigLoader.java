package net.nekonium.explorer;

import net.nekonium.explorer.util.UnconfiguredPropertyException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigLoader {

    private File configPath;

    private String databaseURL;
    private String databaseUser;
    private String databasePassword;
    private Web3jManager.ConnectionType nodeConnectionType;
    private String nodeURL;

    public ConfigLoader(File configPath) {
        this.configPath = configPath;
    }

    public void loadFromFile() throws IOException, UnconfiguredPropertyException {
        final Properties properties = new Properties();

        properties.load(new FileInputStream(configPath));

        this.databaseURL = properties.getProperty("database.url");
        checkNull("database.url", databaseURL);

        this.databaseUser = properties.getProperty("database.user");
        checkNull("database.user", databaseUser);

        this.databasePassword = properties.getProperty("database.password");
        checkNull("database.password", databasePassword);


        final String connectionTypeStr = properties.getProperty("node.connectiontype");
        try {
            this.nodeConnectionType = Web3jManager.ConnectionType.valueOf(connectionTypeStr);
        } catch (IllegalArgumentException e) {
            throw new UnconfiguredPropertyException("node.connectiontype references unknown connection type");
        }

        this.nodeURL = properties.getProperty("node.url");
        checkNull("node.url", nodeURL);

    }

    private void checkNull(String propertyName, Object property) throws UnconfiguredPropertyException {
        if (property == null) {
            throw new UnconfiguredPropertyException(propertyName + " is null");
        }
    }


    public String getDatabaseURL() {
        return databaseURL;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public String getDatabasePassword() {
        return databasePassword;
    }


    public Web3jManager.ConnectionType getNodeConnectionType() {
        return nodeConnectionType;
    }

    public String getNodeURL() {
        return nodeURL;
    }


    public File getConfigPath() {
        return configPath;
    }
}
