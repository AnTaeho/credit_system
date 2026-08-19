package com.example.credit_system.job.concurrency;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.test.context.DynamicPropertyRegistry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

abstract class SharedContainers {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit")
            .withReuse(true);

    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379)
                    .withReuse(true);

    static {
        MYSQL.start();
        REDIS.start();
        flushRedis();
    }

    private static void flushRedis() {
        try {
            REDIS.execInContainer("redis-cli", "FLUSHALL");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush redis", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to flush redis", e);
        }
    }

    static void registerDatabase(DynamicPropertyRegistry registry, String database) {
        createDatabase(database);
        registry.add("spring.datasource.url", () -> jdbcUrlFor(database));
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    private static void createDatabase(String database) {
        String rootUrl = jdbcUrlFor("mysql");
        try (Connection connection = DriverManager.getConnection(rootUrl, "root", MYSQL.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + database);
            statement.execute("GRANT ALL PRIVILEGES ON " + database + ".* TO 'credit'@'%'");
            statement.execute("FLUSH PRIVILEGES");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create database " + database, e);
        }
    }

    private static String jdbcUrlFor(String database) {
        String jdbcUrl = MYSQL.getJdbcUrl();
        int schemeEnd = jdbcUrl.indexOf("://") + 3;
        int hostEnd = jdbcUrl.indexOf('/', schemeEnd);
        int paramsStart = jdbcUrl.indexOf('?', hostEnd);
        String prefix = jdbcUrl.substring(0, hostEnd + 1);
        String params = paramsStart == -1 ? "" : jdbcUrl.substring(paramsStart);
        return prefix + database + params;
    }
}
