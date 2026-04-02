package com.hjazsbjava;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class OrderFunctions {
    private static final String ENV_NAMESPACE = "SERVICEBUS_NAMESPACE";
    private static final String ENV_QUEUE = "SERVICEBUS_QUEUE_NAME";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @FunctionName("sendOrder")
    public HttpResponseMessage sendOrder(
        @HttpTrigger(
            name = "req",
            methods = {HttpMethod.POST},
            authLevel = AuthorizationLevel.FUNCTION,
            route = "orders/send") HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

        try {
            Order order = parseOrDefaultOrder(request.getBody().orElse(null));
            validateOrder(order);
            String body = MAPPER.writeValueAsString(order);

            Config config = readConfig();
            try (ServiceBusSenderClient sender = createSender(config)) {
                sender.sendMessage(new com.azure.messaging.servicebus.ServiceBusMessage(body));
            }
            return request.createResponseBuilder(HttpStatus.OK)
                .body("Message sent to queue '" + config.queueName + "' for orderId='" + order.orderId + "'.")
                .build();
        } catch (IllegalStateException ex) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body(ex.getMessage()).build();
        } catch (Exception ex) {
            context.getLogger().severe("Failed to send message: " + ex.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Failed to send message.")
                .build();
        }
    }

    @FunctionName("receiveOrder")
    public HttpResponseMessage receiveOrder(
        @HttpTrigger(
            name = "req",
            methods = {HttpMethod.GET},
            authLevel = AuthorizationLevel.FUNCTION,
            route = "orders/receive") HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

        try {
            Config config = readConfig();
            try (ServiceBusReceiverClient receiver = createReceiver(config)) {
                Iterable<ServiceBusReceivedMessage> messages = receiver.receiveMessages(1, Duration.ofSeconds(10));
                for (ServiceBusReceivedMessage msg : messages) {
                    String payload = msg.getBody().toString();
                    Order order = tryParseOrder(payload);
                    receiver.complete(msg);
                    return request.createResponseBuilder(HttpStatus.OK)
                        .body(order != null
                            ? "Received and completed: orderId='" + order.orderId + "', status='" + order.status + "'."
                            : "Received and completed: " + payload)
                        .build();
                }
            }
            return request.createResponseBuilder(HttpStatus.NO_CONTENT)
                .body("No message available.")
                .build();
        } catch (IllegalStateException ex) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body(ex.getMessage()).build();
        } catch (Exception ex) {
            context.getLogger().severe("Failed to receive message: " + ex.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Failed to receive message.")
                .build();
        }
    }

    private static ServiceBusSenderClient createSender(Config config) {
        return new ServiceBusClientBuilder()
            .credential(config.namespace, new DefaultAzureCredentialBuilder().build())
            .sender()
            .queueName(config.queueName)
            .buildClient();
    }

    private static ServiceBusReceiverClient createReceiver(Config config) {
        return new ServiceBusClientBuilder()
            .credential(config.namespace, new DefaultAzureCredentialBuilder().build())
            .receiver()
            .queueName(config.queueName)
            .buildClient();
    }

    private static Order parseOrDefaultOrder(String requestBody) throws Exception {
        if (requestBody == null || requestBody.isBlank()) {
            return defaultOrder();
        }
        return MAPPER.readValue(requestBody, Order.class);
    }

    private static Order tryParseOrder(String body) {
        try {
            return MAPPER.readValue(body, Order.class);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void validateOrder(Order order) {
        if (order == null) {
            throw new IllegalStateException("Invalid order payload.");
        }
        if (order.orderId == null || order.orderId.isBlank()) {
            throw new IllegalStateException("orderId is required.");
        }
        if (order.customerId == null || order.customerId.isBlank()) {
            throw new IllegalStateException("customerId is required.");
        }
        if (order.restaurantId == null || order.restaurantId.isBlank()) {
            throw new IllegalStateException("restaurantId is required.");
        }
        if (order.items == null || order.items.isEmpty()) {
            throw new IllegalStateException("At least one item is required.");
        }
        if (order.status == null || order.status.isBlank()) {
            throw new IllegalStateException("status is required.");
        }
    }

    private static Order defaultOrder() {
        OrderItem item = new OrderItem();
        item.itemId = "item-1";
        item.name = "Cheese Burger";
        item.quantity = 1;
        item.unitPrice = 8.99;
        item.notes = "No onions";

        Order order = new Order();
        order.orderId = "demo-" + UUID.randomUUID();
        order.customerId = "cust-1001";
        order.restaurantId = "rest-2001";
        order.items = new ArrayList<>();
        order.items.add(item);
        order.subtotal = 8.99;
        order.deliveryFee = 2.00;
        order.totalAmount = 10.99;
        order.paymentMethod = "CARD";
        order.status = "CREATED";
        order.createdAt = Instant.now().toString();
        return order;
    }

    private static Config readConfig() {
        String namespace = normalizeNamespace(System.getenv(ENV_NAMESPACE));
        String queue = System.getenv(ENV_QUEUE);

        if (namespace == null || namespace.isBlank()) {
            throw new IllegalStateException("Missing environment variable: " + ENV_NAMESPACE);
        }
        if (queue == null || queue.isBlank()) {
            throw new IllegalStateException("Missing environment variable: " + ENV_QUEUE);
        }

        return new Config(namespace, queue);
    }

    private static String normalizeNamespace(String raw) {
        if (raw == null || raw.isBlank()) {
            return raw;
        }
        if (raw.endsWith(".servicebus.windows.net")) {
            return raw;
        }
        return raw + ".servicebus.windows.net";
    }

    private static class Config {
        private final String namespace;
        private final String queueName;

        private Config(String namespace, String queueName) {
            this.namespace = namespace;
            this.queueName = queueName;
        }
    }

    public static class Order {
        public String orderId;
        public String customerId;
        public String restaurantId;
        public List<OrderItem> items;
        public double subtotal;
        public double deliveryFee;
        public double totalAmount;
        public String paymentMethod;
        public String status;
        public String createdAt;
    }

    public static class OrderItem {
        public String itemId;
        public String name;
        public int quantity;
        public double unitPrice;
        public String notes;
    }
}
