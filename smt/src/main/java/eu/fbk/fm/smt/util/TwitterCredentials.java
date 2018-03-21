package eu.fbk.fm.smt.util;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;

/**
 * Class that contains full Twitter credentials needed to access the API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterCredentials implements Serializable {
    public String consumerKey;
    public String consumerSecret;
    public String token;
    public String tokenSecret;

    public static TwitterCredentials[] credentialsFromFile(File file) throws FileNotFoundException {
        Gson gson = new Gson();
        return gson.fromJson(new FileReader(file), TwitterCredentials[].class);
    }
}