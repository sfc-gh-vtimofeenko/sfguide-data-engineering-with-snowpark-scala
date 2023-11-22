package com.snowflake.examples.utils;

import com.snowflake.snowpark_java.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.io.InputStream;

public class LocalSessionHelper {

    public static final String PROPERTIES_FILE_NAME = "snowflake.properties";

    public static Map<String, String> convertPropertiesToMap(Properties properties) {
        Map<String, String> map = new HashMap<>();
        for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            String value = properties.getProperty(key);
            map.put(key, value);
        }
        return map;
    }

    public static File findProjectRootDirectory() {
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));
        File directory = new File("").getAbsoluteFile(); // Start at the current directory

        while (directory != null) {
            File pom = new File(directory, "pom.xml");
            if (pom.exists() && pom.isFile()) {
                return directory; // Found the directory containing pom.xml
            }
            directory = directory.getParentFile(); // Move up to the parent directory
        }

        return null; // Could not find the project root directory
    }

    private static Properties resolveProperties() {
        Properties properties = new Properties();

        // Try to dynamically load properties file from project root
        // Otherwise assume its in current working directory
        File propertiesFile = null;
        File projectRoot = findProjectRootDirectory();
        if (projectRoot != null) {
            System.out.println("Found project root directory");
            propertiesFile = new File(projectRoot, PROPERTIES_FILE_NAME);
        } else {
            System.out.println("Unable to find project root directory");
            propertiesFile = new File(PROPERTIES_FILE_NAME);
        }

        try (InputStream input = new FileInputStream(propertiesFile)) {
            properties.load(input);
            // Get passphrase from environment variable
            String pk_passphrase = System.getenv("PRIVATE_KEY_FILE_PWD");
            if (pk_passphrase != null && !pk_passphrase.isEmpty()) {
                properties.setProperty("PRIVATE_KEY_FILE_PWD", pk_passphrase);
            } else {
                System.out.println("No passphrase was provided");
            }
        } catch (Exception ex) {
            throw new ConfigurationException("Error loading properties from file: " + PROPERTIES_FILE_NAME, ex);
        }
        return properties;
    }


    public static Session buildSnowparkSession() {
        Properties properties = resolveProperties();
        return Session.builder().configs(convertPropertiesToMap(properties)).create();
    }

}