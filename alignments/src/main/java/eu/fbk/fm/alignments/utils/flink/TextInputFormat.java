package eu.fbk.fm.alignments.utils.flink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.charset.Charset;

@PublicEvolving
public class TextInputFormat extends DelimitedInputFormat<String> {

    private static final long serialVersionUID = 1L;

    /**
     * Code of \r, used to remove \r from a line when the line ends with \r\n
     */
    private static final byte CARRIAGE_RETURN = (byte) '\r';

    /**
     * Code of \n, used to identify if \n is used as delimiter
     */
    private static final byte NEW_LINE = (byte) '\n';


    /**
     * The name of the charset to use for decoding.
     */
    private String charsetName = "UTF-8";

    // --------------------------------------------------------------------------------------------

    public TextInputFormat(Path filePath) {
        super(filePath);
    }

    // --------------------------------------------------------------------------------------------

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        if (charsetName == null) {
            throw new IllegalArgumentException("Charset must not be null.");
        }

        this.charsetName = charsetName;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String readRecord(String reusable, byte[] bytes, int offset, int numBytes) throws IOException {
        //Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
        if (this.getDelimiter() != null && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
                && bytes[offset+numBytes-1] == CARRIAGE_RETURN){
            numBytes -= 1;
        }

        return new String(bytes, offset, numBytes, this.charsetName);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "TextInputFormat (" + getFilePath() + ") - " + this.charsetName;
    }
}
