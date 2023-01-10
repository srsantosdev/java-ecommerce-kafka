package pro.samuelramos.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            var order = new Order(userId, orderId, amount);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
        }

        try(var emailDispatcher = new KafkaDispatcher<Email>()) {
            var email = new Email(
                "Thank you for your order!",
                "We are processing the order."
            );

            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
        }
    }
}
