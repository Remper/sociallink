package eu.fbk.fm.alignments.evaluation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * A named dataset of entity -> twitter_id alignments
 */
public class Dataset implements Iterable<DatasetEntry> {

    private static final Logger logger = LoggerFactory.getLogger(Dataset.class);

    private String name = "default";
    private List<DatasetEntry> entries = new LinkedList<>();
    private Map<String, DatasetEntry> mappedEntries = new HashMap<>();

    private Dataset() {
    }

    public List<DatasetEntry> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void add(DatasetEntry entry) {
        if (mappedEntries.containsKey(entry.resourceId)) {
            logger.error("This example is already in the dataset: " + entry.resourceId);
        }
        entries.add(entry);
        mappedEntries.put(entry.resourceId, entry);
    }

    public DatasetEntry findEntry(String resourceId) {
        return mappedEntries.get(resourceId);
    }

    @Override
    public Iterator<DatasetEntry> iterator() {
        return entries.iterator();
    }

    public static Dataset fromFile(File file) throws IOException {
        Dataset dataset = new Dataset();
        dataset.setName(file.getName());
        try (Reader reader = new FileReader(file)) {
            CSVParser parser = new CSVParser(
                    reader,
                    CSVFormat.DEFAULT.withDelimiter(',').withHeader()
            );
            for (CSVRecord record : parser) {
                dataset.add(new DatasetEntry(record.get("entity"), record.get("twitter_id")));
            }
        }
        return dataset;
    }
}
