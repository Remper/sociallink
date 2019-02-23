package eu.fbk.fm.alignments.pipeline;

import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.core.CommandLine;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static eu.fbk.fm.alignments.index.db.tables.Alignments.ALIGNMENTS;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.select;

/**
 * Run postprocessing on the scored pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PostProcess {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreEntities.class);

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";
    private static final String GOLD = "gold";

    private static final String ASSIGNMENT_PROCEDURE_V3 = "" +
        "UPDATE alignments SET is_alignment = true " +
        "FROM ( " +
        "  SELECT b.*, b.max / b.normal_factor AS score " +
        "  FROM ( " +
        "    SELECT a.*, 1-a.max AS negative, 1-a.max+a.sum AS normal_factor " +
        "    FROM ( " +
        "      SELECT resource_id, max(score) AS max, sum(score) AS sum " +
        "      FROM alignments " +
        "      GROUP BY resource_id " +
        "    ) AS a " +
        "  ) AS b " +
        "  WHERE b.max / b.normal_factor > ?) AS c " +
        "WHERE alignments.resource_id = c.resource_id AND alignments.score = c.max;";

    private final DataSource source;

    public PostProcess(DataSource source) {
        this.source = source;
    }

    public void run(float threshold, List<String> goldStandard) {
        DSLContext context = DSL.using(source, SQLDialect.POSTGRES);

        // Run some statistics on the computed dataset
        int datasetSize = 0;
        Result<Record2<Short, Integer>> result = context
            .select(ALIGNMENTS.VERSION, count())
            .from(ALIGNMENTS)
            .groupBy(ALIGNMENTS.VERSION)
            .fetch();
        if (result.size() == 0) {
            LOGGER.info("The database is empty. Nothing to do");
            return;
        }
        LOGGER.info("Database version breakdown: ");
        LOGGER.info("  version | count");
        for (Record2<Short, Integer> element : result) {
            if (element.value1() == 2) {
                datasetSize = element.value2();
            }
            LOGGER.info(String.format("  %7d | %d", element.value1(), element.value2()));
        }

        if (datasetSize == 0) {
            LOGGER.info("No entities has been scored. Nothing else to do");
            return;
        }

        int entities = context.fetchCount(
            select(count()).from(ALIGNMENTS).groupBy(ALIGNMENTS.RESOURCE_ID)
        );
        LOGGER.info(String.format("Found %.2fm scored records (%d unique entities)", (float)datasetSize/1000000, entities));

        // Assign alignment flags
        LOGGER.info("Assigning alignment flags with threshold: "+threshold);
        int updated = context.execute(ASSIGNMENT_PROCEDURE_V3, threshold);
        LOGGER.info(String.format("The amount of alignments: %d", updated));

        // Filter out gold standard entities
        if (goldStandard.size() == 0) {
            LOGGER.info("Gold standard hasn't been loaded, skipping");
            return;
        }
        LOGGER.info("Filtering out gold standard entities from the database (gold size: "+goldStandard.size()+")");
        AtomicInteger deleted =  new AtomicInteger();
        AtomicInteger processed = new AtomicInteger();
        goldStandard.forEach(sample -> {
            int deletedRows = context.delete(ALIGNMENTS).where(ALIGNMENTS.RESOURCE_ID.eq(sample)).execute();
            if (deletedRows > 0) {
                deleted.getAndIncrement();
            }

            int curProcessed = processed.incrementAndGet();
            if (curProcessed % 1000 == 0) {
                LOGGER.info(String.format("Processed %.0fk samples (filtered %d)", (float)curProcessed/1000, deleted.get()));
            }
        });
        LOGGER.info(String.format("Done. %.2fk samples (filtered %d)", (float)processed.get()/1000, deleted.get()));
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("c", DB_CONNECTION,
                        "connection string for the database", "DB",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_USER,
                        "user for the database", "USER",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_PASSWORD,
                        "password for the database", "PASSWORD",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, GOLD,
                        "path to gold standard dataset", "PATH",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String dbConnection = cmd.getOptionValue(DB_CONNECTION, String.class);
            final String dbUser = cmd.getOptionValue(DB_USER, String.class);
            final String dbPassword = cmd.getOptionValue(DB_PASSWORD, String.class);
            final String goldPath = cmd.getOptionValue(GOLD, String.class);

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);

            PostProcess script = new PostProcess(source);

            List<String> goldStandard = new LinkedList<>();
            if (goldPath != null) {
                goldStandard = Files.lines(Paths.get(goldPath)).map(line -> line.split(",")[0]).collect(Collectors.toList());
            }

            script.run(0.3f, goldStandard);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
