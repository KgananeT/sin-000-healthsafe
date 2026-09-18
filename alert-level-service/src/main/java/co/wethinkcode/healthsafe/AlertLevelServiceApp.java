package co.wethinkcode.healthsafe;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AlertLevelServiceApp {

    // 0 = normal, 8 = full Code Blue. Starts at 0 (normal operations).
    private static final AtomicInteger level = new AtomicInteger(0);

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7032);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /alert-level -> current Emergency Status
        app.get("/alert-level", ctx -> ctx.json(Map.of("level", level.get())));

        // PUT /alert-level -> set a new Emergency Status, body: {"level": 0-8}
        app.put("/alert-level", ctx -> {
            LevelUpdate update = ctx.bodyAsClass(LevelUpdate.class);

            if (update.level == null || update.level < 0 || update.level > 8) {
                ctx.status(HttpStatus.BAD_REQUEST)
                        .json(Map.of("error", "level must be an integer between 0 and 8"));
                return;
            }

            level.set(update.level);
            ctx.json(Map.of("level", level.get()));
        });
    }

    static class LevelUpdate {
        public Integer level;

        public LevelUpdate() {
            // needed for Jackson deserialization
        }
    }
}