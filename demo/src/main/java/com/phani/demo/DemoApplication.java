package com.phani.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {

		System.out.println("_______");
		System.out.println(args.length);
		System.out.println("_________________");
		SpringApplication.run(DemoApplication.class, args);
	}

}
