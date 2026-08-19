package com.example.credit_system;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.config.IdempotencyProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties({AppProperties.class, IdempotencyProperties.class})
@SpringBootApplication
public class CreditSystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(CreditSystemApplication.class, args);
	}

}
