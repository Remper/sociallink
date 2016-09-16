package eu.fbk.ict.fm.smt;

import com.google.gson.Gson;
import eu.fbk.ict.fm.smt.db.alignments.tables.Profiles;
import eu.fbk.ict.fm.smt.db.alignments.tables.Resources;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ProfilesRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ResourcesRecord;
import eu.fbk.ict.fm.smt.db.old_alignments.tables.Alignments;
import eu.fbk.ict.fm.smt.db.old_alignments.tables.Users;
import eu.fbk.ict.fm.smt.db.old_alignments.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.db.old_alignments.tables.records.UsersRecord;
import eu.fbk.ict.fm.smt.util.ConnectionFactory;
import eu.fbk.ict.fm.smt.util.TwitterDeserializer;
import org.apache.commons.cli.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * Conversion script between old schema and a new one
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ConvertOldAlignmentsToNew {
    private static final Gson gson = TwitterDeserializer.getDefault().getBuilder().create();

    public ConnectionProvider oldAlignments, alignments;

    public ConvertOldAlignmentsToNew(DataSource oldAlignments, DataSource alignments) {
        this.oldAlignments = new ConnectionFactory(oldAlignments).provide();
        this.alignments = new ConnectionFactory(alignments).provide();
    }

    public void transferProfiles() {
        DSLContext context = DSL.using(oldAlignments, SQLDialect.MYSQL);
        DSLContext newContext = DSL.using(alignments, SQLDialect.MYSQL);
        eu.fbk.ict.fm.smt.db.alignments.tables.Alignments ALIGNMENTS = eu.fbk.ict.fm.smt.db.alignments.tables.Alignments.ALIGNMENTS_;
        Cursor<Record1<Long>> cursor = newContext
            .select(ALIGNMENTS.TWITTER_ID)
            .from(ALIGNMENTS)
            .groupBy(ALIGNMENTS.TWITTER_ID)
            .fetchLazy();

        int processed = 0;
        List<Long> batch = new LinkedList<>();
        for (Record1<Long> record : cursor) {
            batch.add(record.value1());
            processed++;
            if (processed % 10000 == 0) {
                processBatch(context, newContext, batch);
                System.out.println("Processed "+processed+" entities");
            }
        }
        processBatch(context, newContext, batch);
    }

    private Set<String> usernames = new HashSet<>();
    private Set<Long> ids = new HashSet<>();
    private void processBatch(DSLContext context, DSLContext newContext, List<Long> batch) {
        System.out.println("Begin query");
        Result<UsersRecord> result = context
            .selectFrom(Users.USERS)
            .where(Users.USERS.TWITTER_ID.in(batch))
            .fetch();
        System.out.println("Done query");
        List<ProfilesRecord> profiles = new LinkedList<>();
        for (UsersRecord user : result) {
            if (ids.contains(user.getTwitterId())) {
                continue;
            }
            String username = gson.fromJson(user.getObject(), User.class).getScreenName().toLowerCase();
            if (usernames.contains(username)) {
                System.out.println("Duplicate username: "+username+" for ID: "+user.getTwitterId());
                continue;
            }
            ProfilesRecord profile = newContext.newRecord(Profiles.PROFILES);
            profile.setTwitterId(user.getTwitterId());
            usernames.add(username);
            ids.add(user.getTwitterId());
            profile.setUsername(username);
            profiles.add(profile);
        }
        newContext.batchStore(profiles).execute();
        batch.clear();
    }

    public void run() {
        DSLContext context = DSL.using(oldAlignments, SQLDialect.MYSQL);
        DSLContext newContext = DSL.using(alignments, SQLDialect.MYSQL);
        Cursor<AlignmentsRecord> cursor = context.selectFrom(Alignments.ALIGNMENTS).fetchLazy();

        int processed = 0;
        List<eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord> batch = new LinkedList<>();
        List<ResourcesRecord> resourcesBatch = new LinkedList<>();
        for (AlignmentsRecord record : cursor) {
            double[] scores = gson.fromJson(record.getScores(), double[].class);
            long[] candidates = gson.fromJson(record.getCandidates(), long[].class);
            ResourcesRecord resourceRecord = newContext.newRecord(Resources.RESOURCES);
            resourceRecord.setResourceId(record.getResourceId());
            resourceRecord.setIsDead((byte) (record.getSource().equals("Dead") ? 1 : 0));
            resourceRecord.setDataset("default");
            resourcesBatch.add(resourceRecord);
            if (scores == null || candidates == null) {
                continue;
            }
            int min = scores.length;
            if (candidates.length < min) {
                min = candidates.length;
            }
            for (int i = 0; i < min; i++) {
                eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord newRecord = newContext.newRecord(eu.fbk.ict.fm.smt.db.alignments.tables.Alignments.ALIGNMENTS_);
                newRecord.setResourceId(record.getResourceId());
                newRecord.setTwitterId(candidates[i]);
                newRecord.setScore(String.valueOf(scores[i]));
                newRecord.setIsAlignment((byte) (record.getTwitterId().equals(candidates[i]) ? 1 : 0));
                batch.add(newRecord);
            }
            processed++;
            if (processed % 10000 == 0) {
                newContext.batchStore(batch).execute();
                batch.clear();
                System.out.println("Processed "+processed+" entities");
            }
            if (processed % 100000 == 0) {
                newContext.batchStore(resourcesBatch).execute();
                batch.clear();
                System.out.println("Resources saved");
            }
        }
        newContext.batchStore(resourcesBatch).execute();
        newContext.batchStore(batch).execute();
    }

    public static void main(String[] args) throws FileNotFoundException {
        Configuration config = loadConfiguration(args);
        if (config == null) {
            return;
        }

        DataSource oldAlignments = ConnectionFactory.getConf(config.oldAlignments);
        DataSource alignments = ConnectionFactory.getConf(config.alignments);
        ConvertOldAlignmentsToNew script = new ConvertOldAlignmentsToNew(oldAlignments, alignments);
        System.out.println("Moving alignments info");
        Date timestamp = new Date();
        script.run();
        System.out.println(String.format("Done in %.2f seconds", (float)(new Date().getTime() - timestamp.getTime())/1000));
        System.out.println("Moving user profiles");
        timestamp = new Date();
        script.transferProfiles();
        System.out.println(String.format("Done in %.2f seconds", (float)(new Date().getTime() - timestamp.getTime())/1000));
    }

    public static class Configuration {
        public String oldAlignments;
        public String alignments;
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder().desc("Serialized connection to the old alignments database")
                        .required().hasArg().argName("file").longOpt("old").build()
        );
        options.addOption(
                Option.builder().desc("Serialized connection to the new alignments database")
                        .required().hasArg().argName("file").longOpt("new").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            Configuration config = new Configuration();
            config.oldAlignments = line.getOptionValue("old");
            config.alignments = line.getOptionValue("new");
            return config;
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed: " + exp.getMessage() + "\n");
            printHelp(options);
            System.exit(1);
        }
        return null;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                200,
                "java -Dfile.encoding=UTF-8 "+ConvertOldAlignmentsToNew.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }
}
