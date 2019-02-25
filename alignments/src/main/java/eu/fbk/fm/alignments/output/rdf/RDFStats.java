package eu.fbk.fm.alignments.output.rdf;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

import com.google.common.collect.Ordering;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.RDFSource;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.IO;

public final class RDFStats implements Serializable, Comparable<RDFStats> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFStats.class);

    private static final IRI SL_CANDIDATE = Statements.VALUE_FACTORY
            .createIRI("http://sociallink.futuro.media/ontology#candidate");

    private final IRI type;

    private int numEntities;

    private int numEntitiesAligned;

    private int numEntitiesWithCandidates;

    private final SummaryStatistics numCandidatesPerEntity;

    private final SummaryStatistics numAliasesPerEntity;

    private RDFStats(final IRI type) {
        this.type = type;
        this.numEntities = 0;
        this.numEntitiesAligned = 0;
        this.numEntitiesWithCandidates = 0;
        this.numCandidatesPerEntity = new SummaryStatistics();
        this.numAliasesPerEntity = new SummaryStatistics();
    }

    private void addEntity(final boolean linked, final int numCandidates, final int numAliases) {
        ++this.numEntities;
        if (linked) {
            ++this.numEntitiesAligned;
        }
        if (numCandidates != 0) {
            ++this.numEntitiesWithCandidates;
            this.numCandidatesPerEntity.addValue(numCandidates);
        }
        this.numAliasesPerEntity.addValue(numAliases);
    }

    public int getNumEntities() {
        return this.numEntities;
    }

    public int getNumEntitiesAligned() {
        return this.numEntitiesAligned;
    }

    public int getNumEntitiesWithCandidates() {
        return this.numEntitiesWithCandidates;
    }

    public SummaryStatistics getNumCandidatesPerEntity() {
        return this.numCandidatesPerEntity;
    }

    public SummaryStatistics getNumAliasesPerEntity() {
        return this.numAliasesPerEntity;
    }

    @Override
    public int compareTo(final RDFStats other) {
        return this.type.toString().compareTo(other.type.toString());
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof RDFStats)) {
            return false;
        }
        final RDFStats other = (RDFStats) object;
        return this.type.equals(other.type);
    }

    @Override
    public int hashCode() {
        return this.type.hashCode();
    }

    @Override
    public String toString() {
        return this.type + "(" + this.numEntities + " entities, " + this.numEntitiesWithCandidates
                + " w/ candidates, " + this.numEntitiesAligned + " aligned, "
                + this.numCandidatesPerEntity.getMean() + " candidates/entity avg, "
                + this.numAliasesPerEntity.getMean() + " aliases/entity avg)";
    }

    public static Map<IRI, RDFStats> compute(final RDFSource slSource,
            final RDFSource typesSource) {

        // Index #candidates and linked flag for each SocialLink entity (32 bytes/entity avg.)
        final IriToIntMap entityMap = new IriToIntMap();
        final AtomicLong slCounter = new AtomicLong();
        slSource.emit(new AbstractRDFHandler() {

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                final long slCount = slCounter.incrementAndGet();
                if (slCount % 1000000L == 0L) {
                    LOGGER.debug("Processed {} SocialLink triples...", slCount);
                }
                final Resource s = stmt.getSubject();
                final IRI p = stmt.getPredicate();
                if (!(s instanceof IRI)) {
                    return;
                }
                if (p.equals(FOAF.ACCOUNT)) {
                    synchronized (entityMap) {
                        entityMap.update((IRI) s, v -> v | 0x80000000);
                    }
                } else if (p.equals(SL_CANDIDATE)) {
                    synchronized (entityMap) {
                        entityMap.update((IRI) s, v -> v + 1);
                    }
                } else if (p.equals(OWL.SAMEAS)) {
                    synchronized (entityMap) {
                        entityMap.update((IRI) s, v -> v + 0x10000);
                    }
                }
            }

        }, 1);
        LOGGER.debug("Loaded {} entities from {} SocialLink triples", entityMap.size(), slCounter);

        // Scan type information, adding entity data to type stats
        final Map<IRI, RDFStats> stats = Maps.newConcurrentMap();
        final AtomicLong typesCounter = new AtomicLong();
        typesSource.emit(new AbstractRDFHandler() {

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                final long typesCount = typesCounter.incrementAndGet();
                if (typesCount % 1000000L == 0L) {
                    LOGGER.debug("Processed {} types triples...", typesCount);
                }
                if (!RDF.TYPE.equals(stmt.getPredicate()) || !(stmt.getObject() instanceof IRI)
                        || !(stmt.getSubject() instanceof IRI)) {
                    return;
                }
                final IRI s = (IRI) stmt.getSubject();
                final IRI t = (IRI) stmt.getObject();
                final RDFStats ts = stats.computeIfAbsent(t, k -> new RDFStats(k));
                final int v = entityMap.get(s);
                synchronized (ts) {
                    ts.addEntity((v & 0x80000000) != 0, v & 0xFFFF, (v & 0x7FFF0000) >> 16);
                }
            }

        }, 1);
        LOGGER.debug("Computed statistics for {} types based on {} types triples", stats.size(),
                typesCounter);

        // Return statistics
        return stats;
    }

    public static void writeTsv(final Path file, final Iterable<RDFStats> stats)
            throws IOException {
        try (Writer out = IO.utf8Writer(IO.buffer(IO.write(file.toString())))) {
            out.append("type\tent\tent_aligned\tent_cand\t"
                    + "cand_avg\tcand_stddev\tcand_min\tcand_max\t"
                    + "alias_avg\talias_stddev\talias_min\talias_max\n");
            for (final RDFStats s : stats) {
                final SummaryStatistics nc = s.numCandidatesPerEntity;
                final SummaryStatistics na = s.numAliasesPerEntity;
                out.append(s.type.toString()).append('\t');
                out.append(Integer.toString(s.numEntities)).append('\t');
                out.append(Integer.toString(s.numEntitiesAligned)).append('\t');
                out.append(Integer.toString(s.numEntitiesWithCandidates)).append('\t');
                out.append(Double.toString(nc.getMean())).append('\t');
                out.append(Double.toString(nc.getStandardDeviation())).append('\t');
                out.append(Double.toString(nc.getMin())).append('\t');
                out.append(Double.toString(nc.getMax())).append('\t');
                out.append(Double.toString(na.getMean())).append('\t');
                out.append(Double.toString(na.getStandardDeviation())).append('\t');
                out.append(Double.toString(na.getMin())).append('\t');
                out.append(Double.toString(na.getMax())).append('\n');
            }
        }
    }

    public static void main(final String... args) {
        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser()
                    .withOption("s", "sociallink",
                            "the SocialLink FILE containing the RDF of SocialLink Dataset", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("t", "types",
                            "the types FILE mapping each entity to its RDF types", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("o", "output",
                            "the output FILE where TSV statistics will be written", "FILE",
                            CommandLine.Type.FILE, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk.fm.alignments.output.rdf"))
                    .parse(args);

            // Read paths
            final Path slFile = cmd.getOptionValue("s", Path.class);
            final Path typesFile = cmd.getOptionValue("t", Path.class);
            final Path outputFile = cmd.getOptionValue("o", Path.class);

            // Create output directory, if needed
            final Path outputPath = outputFile.getParent();
            if (outputPath != null) {
                Files.createDirectories(outputPath);
            }

            // Create RDF sources
            LOGGER.debug("Reading SocialLink data from {}", slFile);
            final RDFSource slSource = RDFSources.read(true, true, null, null, null, true,
                    slFile.toAbsolutePath().toString());
            LOGGER.debug("Reading types data from {}", typesFile);
            final RDFSource typesSource = RDFSources.read(true, true, null, null, null, true,
                    typesFile.toAbsolutePath().toString());

            // Compute statistics
            final Map<IRI, RDFStats> stats = compute(slSource, typesSource);

            // Emit statistics
            writeTsv(outputFile, Ordering.natural().sortedCopy(stats.values()));
            LOGGER.debug("Written statistics to {}", outputFile);

        } catch (final Throwable ex) {
            // Handle failure
            CommandLine.fail(ex);
        }
    }

    private static final class IriToIntMap {

        private long[] table;

        private int size;

        private IriToIntMap() {
            this.table = new long[16 * 1024 * 1024];
            this.size = 0;
        }

        public int get(final IRI entityIri) {
            final int index = locate(entityIri, false);
            return index < 0 ? 0 : (int) (this.table[index * 2 + 1] & 0xFFFFFFFFL);
        }

        public void update(final IRI entityIri, final IntUnaryOperator updateFunction) {
            final int index = locate(entityIri, true);
            final int offset = index * 2 + 1;
            final int oldValue = (int) (this.table[offset] & 0xFFFFFFFFL);
            final int newValue = updateFunction.applyAsInt(oldValue);
            this.table[offset] = this.table[offset] & 0xFFFFFFFF00000000L | newValue & 0xFFFFFFFFL;
        }

        public int size() {
            return this.size;
        }

        public int locate(final IRI entityIri, final boolean allocate) {
            final String entityIriStr = entityIri.toString();
            final byte[] h = Hashing.murmur3_128().hashUnencodedChars(entityIriStr).asBytes();
            final long h0 = Longs.fromBytes(h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
            final long h1 = Longs.fromBytes(h[8], h[9], h[10], h[11], h[12], h[13], h[14], h[15]);
            return locate(h0, h1, allocate);
        }

        private int locate(long h0, long h1, final boolean allocate) {
            h0 = h0 | 0x8000000000000000L;
            h1 = h1 & 0xFFFFFFFF00000000L;
            int index = (int) ((h0 & 0x7FFFFFFFFFFFFFFFL) % (this.table.length / 2));
            while (true) {
                final int off = index * 2;
                if (this.table[off] == h0 && (this.table[off + 1] & 0xFFFFFFFF00000000L) == h1) {
                    return index;
                } else if (this.table[off] != 0L) {
                    index = (index + 1) % (this.table.length / 2);
                } else if (!allocate) {
                    return -1;
                } else if (this.size < this.table.length / 4) { // 50% fill rate
                    this.table[off] = h0;
                    this.table[off + 1] = h1;
                    ++this.size;
                } else {
                    final long[] oldTable = this.table;
                    this.table = new long[oldTable.length * 2];
                    for (int i = 0; i < oldTable.length / 2; ++i) {
                        final long t0 = oldTable[i * 2];
                        final long t1 = oldTable[i * 2 + 1];
                        final int j = locate(t0, t1, true);
                        this.table[j * 2 + 1] |= t1 & 0xFFFFFFFFL;
                    }
                    return locate(h0, h1, allocate);
                }
            }
        }

    }

}
