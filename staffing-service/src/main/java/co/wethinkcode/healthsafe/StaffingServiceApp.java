package co.wethinkcode.healthsafe;

import co.wethinkcode.healthsafe.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class StaffingServiceApp {

    private static final String WARD_SERVICE_URL = "http://localhost:7031";
    private static final String ALERT_LEVEL_SERVICE_URL = "http://localhost:7032";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    // MQ producer, set up once at startup and reused for every publish.
    private static Session mqSession;
    private static MessageProducer mqProducer;

    public static void main(String[] args) {
        setUpMqProducer();

        Javalin app = Javalin.create().start(7033);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /schedule/{wardId} -> on-call schedule for that ward, based on current Emergency Status
        app.get("/schedule/{wardId}", ctx -> {
            String wardId = ctx.pathParam("wardId").trim().toUpperCase();

            WardRecord ward;
            try {
                ward = fetchWard(wardId);
            } catch (WardNotFoundException e) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "no ward found for id " + wardId));
                return;
            } catch (Exception e) {
                ctx.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .json(Map.of("error", "could not reach ward-service: " + e.getMessage()));
                return;
            }

            int alertLevel;
            try {
                alertLevel = fetchAlertLevel();
            } catch (Exception e) {
                ctx.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .json(Map.of("error", "could not reach alert-level-service: " + e.getMessage()));
                return;
            }

            int doctorsOnCall = computeDoctorsOnCall(alertLevel);

            Map<String, Object> result = Map.of(
                    "wardId", ward.wardId,
                    "department", ward.department,
                    "alertLevel", alertLevel,
                    "doctorsOnCall", doctorsOnCall
            );

            publishStaffingEvent(result);

            ctx.json(result);
        });
    }

    /**
     * Base staffing of 2 doctors, +1 doctor per Emergency Status level
     * (0 = normal -> 2 doctors, 8 = full Code Blue -> 10 doctors).
     */
    private static int computeDoctorsOnCall(int alertLevel) {
        int baseStaff = 2;
        return baseStaff + alertLevel;
    }

    private static WardRecord fetchWard(String wardId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(WARD_SERVICE_URL + "/wards/" + wardId))
                .GET().build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 404) {
            throw new WardNotFoundException(wardId);
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("ward-service returned status " + response.statusCode());
        }
        return MAPPER.readValue(response.body(), WardRecord.class);
    }

    private static int fetchAlertLevel() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(ALERT_LEVEL_SERVICE_URL + "/alert-level"))
                .GET().build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("alert-level-service returned status " + response.statusCode());
        }
        Map<String, Integer> body = MAPPER.readValue(response.body(), Map.class);
        return body.get("level");
    }

    /** Sets up a long-lived JMS connection/session/producer for the staffing-events-topic. */
    private static void setUpMqProducer() {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            mqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = mqSession.createTopic(MqConfig.TOPIC);
            mqProducer = mqSession.createProducer(topic);
            System.out.println("Connected to broker at " + MqConfig.BROKER_URL + ", publishing to " + MqConfig.TOPIC);
        } catch (Exception e) {
            // Don't crash the service if the broker isn't up - REST endpoints should
            // still work even if the MQ side is degraded; we just log and skip publishing.
            System.err.println("WARNING: could not connect to broker (" + e.getMessage() + "). Staffing events will not be published.");
        }
    }

    /** Publishes a staffing schedule/status change as a JSON event. Best-effort - failures are logged, not thrown. */
    private static void publishStaffingEvent(Map<String, Object> event) {
        if (mqProducer == null) {
            return; // broker wasn't available at startup
        }
        try {
            String json = MAPPER.writeValueAsString(event);
            TextMessage message = mqSession.createTextMessage(json);
            mqProducer.send(message);
        } catch (Exception e) {
            System.err.println("WARNING: failed to publish staffing event: " + e.getMessage());
        }
    }

    static class WardRecord {
        public String wardId;
        public String wing;
        public String department;
        public Integer bedsAvailable;
        public String notes;

        public WardRecord() {
        }
    }

    static class WardNotFoundException extends Exception {
        WardNotFoundException(String wardId) {
            super("ward not found: " + wardId);
        }
    }
}