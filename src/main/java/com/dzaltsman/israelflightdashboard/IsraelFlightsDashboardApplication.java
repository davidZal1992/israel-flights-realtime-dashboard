package com.dzaltsman.israelflightdashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IsraelFlightsDashboardApplication {

	public static void main(String[] args) {
		SpringApplication.run(IsraelFlightsDashboardApplication.class, args);
	}

}
