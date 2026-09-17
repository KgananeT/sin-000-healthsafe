package co.wethinkcode.healthsafe;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WardServiceApp {

    private static final String INGESTION_URL = "http://localhost:7030/wards";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // In-memory store of wards, keyed by wardId, populated from ingestion-service at startup.
    private static final Map<String, WardRecord> wardsById = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        loadWardsFromIngestion();

        Javalin app = Javalin.create().start(7031);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /wards -> all cleaned ward records
        app.get("/wards", ctx -> ctx.json(List.copyOf(wardsById.values())));

        // GET /wards/{id} -> a single ward, 404 if unknown
        app.get("/wards/{id}", ctx -> {
            String id = ctx.pathParam("id").trim().toUpperCase();
            WardRecord ward = wardsById.get(id);
            if (ward == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "no ward found for id " + id));
            } else {
                ctx.json(ward);
            }
        });
    }

    /**
     * Fetches the cleaned ward list from ingestion-service on startup. Retries a
     * few times with a short delay, since ingestion-service may not be up yet
     * when ward-service starts (no strict startup ordering between services).
     */
    private static void loadWardsFromIngestion() {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(INGESTION_URL)).GET().build();

        int attempts = 5;
        for (int i = 1; i <= attempts; i++) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    List<WardRecord> wards = MAPPER.readValue(response.body(), new com.fasterxml.jackson.core.type.TypeReference<List<WardRecord>>() {});
                    for (WardRecord ward : wards) {
                        wardsById.put(ward.wardId, ward);
                    }
                    System.out.println("Loaded " + wardsById.size() + " wards from ingestion-service");
                    return;
                }
                System.err.println("ingestion-service returned status " + response.statusCode());
            } catch (Exception e) {
                System.err.println("Attempt " + i + "/" + attempts + ": could not reach ingestion-service (" + e.getMessage() + ")");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        System.err.println("WARNING: could not load wards from ingestion-service after " + attempts + " attempts. Starting with an empty ward list.");
    }

    /** Mirrors the shape produced by ingestion-service's /wards endpoint. */
    static class WardRecord {
        public String wardId;
        public String wing;
        public String department;
        public Integer bedsAvailable;
        public String notes;

        public WardRecord() {
            // needed for Jackson deserialization
        }
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.healthsafe.mq.MqConfig)
// MQ TODO: publishes to ActiveMQ queue MqConfig.QUEUE when it detects an equipment failure on one of its wards.