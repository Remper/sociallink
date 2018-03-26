package eu.fbk.fm.profiling.extractors.LSA;

import eu.fbk.utils.core.core.MultiSet;
import eu.fbk.utils.lsa.AbstractLSI;
import eu.fbk.utils.lsa.ScoreTermMap;
import eu.fbk.utils.lsa.TermNotFoundException;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.SparseVector;
import eu.fbk.utils.math.Vector;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Maps text into the latent semantic space.
 * <p>
 * This class is equals to LSM but uses Vector instead of Node.
 *
 * @version %I%, %G%
 * @author Claudio Giuliano
 * @since 1.0
 */
public class LSM extends AbstractLSI {

    /**
     * Define a static logger variable so that it references the
     * Logger instance named <code>LSM</code>.
     */
    static Logger logger = Logger.getLogger(LSM.class.getName());

    public LSM(String root, int dim, boolean rescaleIdf) throws IOException {
        super(root, dim, rescaleIdf);
    }

    public LSM(String root, int dim, boolean rescaleIdf, boolean normalize) throws IOException {
        super(root, dim, rescaleIdf, normalize);
    }

    public LSM(File UtFile, File SFile, File rowFile, File colFile, File dfFile, int dim, boolean rescaleIdf)
            throws IOException {
        super(UtFile, SFile, rowFile, colFile, dfFile, dim, rescaleIdf);
    }

    public LSM(File UtFile, File SFile, File rowFile, File colFile, File dfFile, int dim, boolean rescaleIdf,
               boolean normalize) throws IOException {
        super(UtFile, SFile, rowFile, colFile, dfFile, dim, rescaleIdf, normalize);
    }

    /**
     * Returns a term in the VSM
     */
    public Vector mapTerm(String term) throws TermNotFoundException {
        int i = termIndex.get(term);

        if (i == -1) {
            throw new TermNotFoundException(term);
        }

        return new DenseVector(Uk[i]);
    } // end mapTerm

    /**
     * Returns a document in the VSM.
     */
    public Vector mapDocument(BOW bow, boolean b) {
        //logger.info("lsm.mapDocument " + b);
        SparseVector vector = new SparseVector();

        Iterator<String> it = bow.termSet().iterator();
        for (int i = 0; it.hasNext(); i++) {
            //logger.info(i + " " + t[i]);
            String term = it.next();
            int index = termIndex.get(term);

            if (index != -1) {
                int tf = bow.getFrequency(term);
                float tfIdf = (float) (log2(tf));
                if (b) {
                    tfIdf *= Iidf[index];
                }

                //logger.info(term + " ==> " + index + ", tf.idf = " + tf + "(" + (log2(tf)) + ") * " + Iidf[index] + " = " + tfIdf);
                vector.add(index, tfIdf);

            }
        } // end for

        return vector;
    } // end map

    /**
     * Returns a document in the VSM.
     */
    public Vector mapDocument(BOW bow) {
        //logger.info("lsm.mapDocument");
        SparseVector vector = new SparseVector();

        Iterator<String> it = bow.termSet().iterator();
        String term = null;
        int index = 0;
        //int tf = 0;
        float tfIdf;
        for (int i = 0; it.hasNext(); i++) {
            //logger.info(i + " " + t[i]);
            term = it.next();
            index = termIndex.get(term);

            if (index != -1) {
                //tf = bow.getFrequency(term);
                //tfIdf = (float) (log2(tf)) * Iidf[index];
                //tfIdf = (float) bow.augmentedFrequency(term) * Iidf[index];
                tfIdf = (float) bow.tf(term) * Iidf[index];
                //logger.info(term + " ==> " + index + ", tf.idf = " + tf + "(" + (log2(tf)) + ") * " + Iidf[index] + " = " + tfIdf);
                vector.add(index, tfIdf);

            }
        } // end for

        return vector;
    } // end map

    /**
     * Returns a document in the VSM.
     */
    public Vector mapDocument(MultiSet<String> bow) {
        //logger.info("lsm.mapDocument");
        SparseVector vector = new SparseVector();

        Iterator<String> it = bow.iterator();
        String term = null;
        int index = 0;
        int tf = 0;
        float tfIdf;
        for (int i = 0; it.hasNext(); i++) {
            //logger.info(i + " " + t[i]);
            term = it.next();
            index = termIndex.get(term);

            if (index != -1) {
                tf = bow.getFrequency(term);
                tfIdf = (float) (log2(tf)) * Iidf[index];

                //logger.info(term + " ==> " + index + ", tf.idf = " + tf + "(" + (log2(tf)) + ") * " + Iidf[index] + " = " + tfIdf);
                vector.add(index, tfIdf);

            }
        } // end for

        return vector;
    } // end map

    /**
     * Returns a document in the latent semantic space.
     */
    public Vector mapPseudoDocument(Vector doc) {
        //logger.info("lsm.mapPseudoDocument " + doc);
        // N = Uk.rows();
        float[] pdoc = new float[Uk[0].length];

        //logger.info("Uk.size " + Uk.length + " X " + Uk[0].length);
        //logger.info("doc.size " + doc.size());
        //logger.info("pdoc.size " + pdoc.length);

        //for (int j=0;j<doc.size();j++)
        //	logger.info(j + " " + doc.get(j));

        for (int i = 0; i < Uk[0].length; i++) {
            Iterator<Integer> it = doc.nonZeroElements();
            for (int j = 0; it.hasNext(); j++) {
                Integer index = it.next().intValue();
                //logger.info(i + ", i: " + index);
                //logger.info(i + ", v:" + doc.get(index));
                //logger.info(i + ", Uk: " + Uk[index][i]);
                pdoc[i] += Uk[index][i] * doc.get(index);

            } // end for j
        } // end for i

        //logger.info("pdoc.size " + pdoc.length);
        return new DenseVector(pdoc);
    } // end mapPseudoDocument

    public void interactive() throws IOException {
        InputStreamReader reader = null;
        BufferedReader myInput = null;
        while (true) {
            logger.info("\nPlease write a query and type <return> to continue (CTRL C to exit):");
            reader = new InputStreamReader(System.in);
            myInput = new BufferedReader(reader);
            String query = myInput.readLine().toString();

            if (query.contains("\t")) {
                // compare two terms
                String[] s = query.split("\t");
                long begin = System.nanoTime();

                //BOW bow1 = new BOW(s[0].toLowerCase().replaceAll("category:", "_").split("[_ ]"));
                //BOW bow2 = new BOW(s[1].toLowerCase().replaceAll("category:", "_").split("[_ ]"));
                BOW bow1 = new BOW(s[0].toLowerCase());
                BOW bow2 = new BOW(s[1].toLowerCase());
                long end = System.nanoTime();

                logger.info("time required " + df.format(end - begin) + " ns");
                begin = System.nanoTime();
                Vector d1 = mapDocument(bow1);
                //logger.info("d1:" + d1);

                Vector d2 = mapDocument(bow2);
                //logger.info("d2:" + d2);

                Vector pd1 = mapPseudoDocument(d1);
                //logger.info("pd1:" + pd1);

                Vector pd2 = mapPseudoDocument(d2);
                //logger.info("pd2:" + pd2);

                double cosVSM = d1.dotProduct(d2) / Math.sqrt(d1.dotProduct(d1) * d2.dotProduct(d2));
                double cosLSM = pd1.dotProduct(pd2) / Math.sqrt(pd1.dotProduct(pd1) * pd2.dotProduct(pd2));
                end = System.nanoTime();
                logger.info("bow1:" + bow1);
                logger.info("bow2:" + bow2);
                logger.info("time required " + df.format(end - begin) + " ns");

                logger.info("<\"" + s[0] + "\",\"" + s[1] + "\"> = " + cosLSM + " (" + cosVSM + ")");

            } else {
                //return the similar terms

                try {
                    query = query.toLowerCase();
                    logger.debug("query " + query);
                    long begin = System.nanoTime();
                    ScoreTermMap map = new ScoreTermMap(query, 20);
                    Vector vec1 = mapTerm(query);

                    String term = null;
                    Iterator<String> it = terms();
                    while (it.hasNext()) {
                        term = it.next();
                        Vector vec2 = mapTerm(term);
                        double cos = vec1.dotProduct(vec2) / Math.sqrt(vec1.dotProduct(vec1) * vec2.dotProduct(vec2));
                        map.put(cos, term);
                    }
                    long end = System.nanoTime();

                    logger.info(map.toString());
                    logger.info("time required " + df.format(end - begin) + " ns");

                } catch (TermNotFoundException e) {
                    logger.error(e);
                }

            }

        } // end while(true)
    }

    public static void main(String[] args) throws Exception {
        String logConfig = System.getProperty("log-config");
        if (logConfig == null) {
            logConfig = "log-config.txt";
        }

        long begin = System.currentTimeMillis();

        PropertyConfigurator.configure(logConfig);

        if (args.length != 5) {
            logger.info(getHelp());
            System.exit(1);
        }

        File Ut = new File(args[0] + "-Ut");
        File Sk = new File(args[0] + "-S");
        File r = new File(args[0] + "-row");
        File c = new File(args[0] + "-col");
        File df = new File(args[0] + "-df");
        double threshold = Double.parseDouble(args[1]);
        int size = Integer.parseInt(args[2]);
        int dim = Integer.parseInt(args[3]);
        boolean rescaleIdf = Boolean.parseBoolean(args[4]);

        LSM LSM = new LSM(Ut, Sk, r, c, df, dim, rescaleIdf);

        LSM.interactive();

        long end = System.currentTimeMillis();
        logger.info("term similarity calculated in " + (end - begin) + " ms");
    } // end main

    /**
     * Returns a command-line help.
     * <p>
     * return a command-line help.
     */
    private static String getHelp() {
        StringBuffer sb = new StringBuffer();

        // License
        //sb.append(License.get());

        // Usage
        sb.append("Usage: java -cp dist/jcore.jar -mx2G eu.fbk.fm.profiling.extractors.LSA.LSM input threshold size dim idf\n\n");

        // Arguments
        sb.append("Arguments:\n");
        sb.append("\tinput\t\t-> root of files from which to read the model\n");
        sb.append("\tthreshold\t-> similarity threshold\n");
        sb.append("\tsize\t\t-> number of similar terms to return\n");
        sb.append("\tdim\t\t-> number of dimensions\n");
        sb.append("\tidf\t\t-> if true rescale using the idf\n");
        //sb.append("\tterm\t\t-> input term\n");

        // Arguments
        //sb.append("Arguments:\n");

        return sb.toString();
    }

} // end class LSM
