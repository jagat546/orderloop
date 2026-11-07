package app.orderloop.fulfilment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // The simulator moves demo orders on by itself
public class FulfilmentServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(FulfilmentServiceApplication.class, args);
  }
}
