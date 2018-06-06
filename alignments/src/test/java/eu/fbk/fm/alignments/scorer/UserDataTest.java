package eu.fbk.fm.alignments.scorer;

import com.google.gson.Gson;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import twitter4j.User;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.*;

public class UserDataTest {

    @org.junit.Test
    public void get() throws IOException {
        UserData user = testUser();
        assertFalse(user.get(new UserData.DataProvider<User>() {

            @Override
            public String getId() {
                return "user";
            }

            @Override
            public User provide(User profile) {
                return profile;
            }
        }).isPresent());
    }

    public UserData testUser() throws IOException {
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();
        User user;
        try (Reader reader = new InputStreamReader(UserDataTest.class.getResourceAsStream("/user.json"))) {
            user = gson.fromJson(reader, User.class);
        }
        return new UserData(user);
    }
}