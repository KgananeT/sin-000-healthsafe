package co.wethinkcode.healthsafe;

import io.javalin.Javalin;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class IngestionServiceApp {

    public static void main(String[] args) throws IOException, com.opencsv.exceptions.CsvException {
        List<WardRecord> wards = loadAndCleanWards("src/main/resources/wards-outdated.csv");

        Javalin app = Javalin.create().start(7030);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /wards -> cleaned ward records, for ward-service to consume
        app.get("/wards", ctx -> ctx.json(wards));
    }

    /**
     * Reads wards-outdated.csv, cleans each row, and merges duplicate wards
     * (same wardId, different casing/values) into a single record.
     */
    static List<WardRecord> loadAndCleanWards(String csvPath) throws IOException, com.opencsv.exceptions.CsvException {
        // Keyed by normalized wardId so duplicates collapse into one entry.
        Map<String, WardRecord> byWardId = new LinkedHashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            List<String[]> rows = reader.readAll();

            // First row is the header (ward_id, Wing, department, beds_available) - skip it.
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);
                WardRecord cleaned = cleanRow(row);

                WardRecord existing = byWardId.get(cleaned.wardId);
                if (existing == null) {
                    byWardId.put(cleaned.wardId, cleaned);
                } else {
                    byWardId.put(cleaned.wardId, mergeDuplicate(existing, cleaned));
                }
            }
        }

        return byWardId.values().stream().toList();
    }

    /** Cleans a single raw CSV row into a WardRecord. */
    private static WardRecord cleanRow(String[] row) {
        String rawWardId = row[0];
        String rawWing = row[1];
        String rawDepartment = row[2];
        String rawBeds = row[3];

        String wardId = rawWardId.trim().toUpperCase();

        String wing = rawWing.trim();
        String wingNote = null;
        if (wing.isEmpty()) {
            wing = null;
            wingNote = "wing was missing";
        } else {
            wing = titleCase(wing);
        }

        String department = titleCase(rawDepartment.trim());

        Integer bedsAvailable = null;
        String bedsNote = null;
        String bedsTrimmed = rawBeds.trim();
        try {
            int parsed = Integer.parseInt(bedsTrimmed);
            if (parsed < 0) {
                bedsNote = "bedsAvailable was negative (" + bedsTrimmed + ") - flagged for follow-up";
            } else {
                bedsAvailable = parsed;
            }
        } catch (NumberFormatException e) {
            bedsNote = "bedsAvailable was non-numeric ('" + bedsTrimmed + "') - flagged for follow-up";
        }

        String note = combineNotes(wingNote, bedsNote);

        return new WardRecord(wardId, wing, department, bedsAvailable, note);
    }

    /**
     * Merges two records that share a wardId: prefer whichever value is
     * already known (non-null) on each field, and record that a merge happened.
     */
    private static WardRecord mergeDuplicate(WardRecord first, WardRecord second) {
        String wing = first.wing != null ? first.wing : second.wing;
        String department = first.department != null ? first.department : second.department;
        Integer beds = first.bedsAvailable != null ? first.bedsAvailable : second.bedsAvailable;

        String mergeNote = "merged duplicate record for " + first.wardId;
        String note = combineNotes(mergeNote, combineNotes(first.notes, second.notes));

        return new WardRecord(first.wardId, wing, department, beds, note);
    }

    private static String combineNotes(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        return a + "; " + b;
    }

    private static String titleCase(String value) {
        String[] words = value.trim().split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (sb.length() > 0) sb.append(" ");
            sb.append(Character.toUpperCase(word.charAt(0)))
                    .append(word.substring(1).toLowerCase());
        }
        return sb.toString();
    }

    /** Cleaned ward record - shape matches the worked example in the README. */
    static class WardRecord {
        public String wardId;
        public String wing;
        public String department;
        public Integer bedsAvailable;
        public String notes;

        public WardRecord(String wardId, String wing, String department, Integer bedsAvailable, String notes) {
            this.wardId = wardId;
            this.wing = wing;
            this.department = department;
            this.bedsAvailable = bedsAvailable;
            this.notes = notes;
        }
    }
}