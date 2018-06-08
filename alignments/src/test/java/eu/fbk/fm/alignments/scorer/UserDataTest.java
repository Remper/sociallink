package eu.fbk.fm.alignments.scorer;

import com.google.gson.Gson;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import twitter4j.User;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.*;

public class UserDataTest {

    public static class MockDataProvider implements UserData.DataProvider<User, Exception> {
        @Override
        public User provide(User profile) {
            return profile;
        }
    }

    public static class statuses implements UserData.DataProvider<User, Exception> {
        @Override
        public User provide(User profile) {
            return profile;
        }
    }

    @org.junit.Test
    public void get() throws Exception {
        UserData user = testUser();
        user.populate(new MockDataProvider());
        user.populate(new statuses());
        assertTrue(user.get(new MockDataProvider()).isPresent());
        assertTrue(user.get(MockDataProvider.class).isPresent());
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