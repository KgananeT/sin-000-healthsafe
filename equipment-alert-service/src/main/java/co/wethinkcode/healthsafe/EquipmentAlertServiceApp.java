package co.wethinkcode.healthsafe;

import co.wethinkcode.healthsafe.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class EquipmentAlertServiceApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // In-memory log of received, successfully-processed alerts - lets us see/verify consumption.
    private static final List<Map<String, Object>> receivedAlerts = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        subscribeToEquipmentFailures();

        Javalin app = Javalin.create().start(7034);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /alerts -> every equipment-failure alert received and processed so far
        app.get("/alerts", ctx -> ctx.json(Collections.unmodifiableList(receivedAlerts)));
    }

    /**
     * Consumes equipment-failure-queue with manual acknowledgement: a message is
     * only ack'd after it's been fully processed (added to receivedAlerts). If
     * processing throws, the message is NOT ack'd, so the broker will redeliver
     * it - either to this consumer on reconnect, or to another consumer if one
     * exists. This is the guaranteed-delivery behaviour a queue provides.
     */
    private static void subscribeToEquipmentFailures() {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();

            // CLIENT_ACKNOWLEDGE: we control exactly when a message is considered "done".
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(MqConfig.QUEUE);
            MessageConsumer consumer = session.createConsumer(queue);

            consumer.setMessageListener(message -> {
                try {
                    if (message instanceof TextMessage textMessage) {
                        String json = textMessage.getText();
                        Map<String, Object> alert = MAPPER.readValue(json, Map.class);

                        receivedAlerts.add(alert);
                        System.out.println("Processed equipment failure alert: " + alert);

                        // Only acknowledge after successful processing - guarantees at-least-once delivery.
                        message.acknowledge();
                    }
                } catch (Exception e) {
                    // Deliberately NOT acknowledging here - message will be redelivered.
                    System.err.println("WARNING: failed to process equipment failure alert, will be redelivered: " + e.getMessage());
                }
            });

            System.out.println("Subscribed to " + MqConfig.QUEUE + " at " + MqConfig.BROKER_URL);
        } catch (Exception e) {
            System.err.println("WARNING: could not connect to broker (" + e.getMessage() + "). Equipment failure alerts will not be received.");
        }
    }
}