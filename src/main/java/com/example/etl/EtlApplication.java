package com.example.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableTransactionManagement
@SpringBootApplication
public class EtlApplication {
	public static void main(String[] args) {
		SpringApplication.run(EtlApplication.class, args);
	}
}
