package pro.samuelramos.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for(var i = 0; i < 10; i++) {
            var orderId = UUID.randomUUID().toString();
            var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
            var email = Math.random() + "@email.com";

            try(var orderDispatcher = new KafkaDispatcher<Order>()) {
                var order = new Order(orderId, amount, email);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);
            }

            try(var emailDispatcher = new KafkaDispatcher<Email>()) {
                var emailContent = new Email(
                    "Thank you for your order!",
                    "We are processing the order."
                );

                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailContent);
            }
        }
    }
}
