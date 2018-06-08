package eu.fbk.fm.alignments.scorer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import org.jetbrains.annotations.NotNull;
import twitter4j.*;

import java.util.*;

/**
 * Data class containing all the information we have on a particular user
 */
public class UserData implements User {
    private static final Gson GSON = TwitterDeserializer.getDefault().getBuilder().create();

    public final User profile;
    protected final Map<String, JsonElement> data = new HashMap<>();

    public UserData(User profile) {
        this.profile = profile;
    }

    public <T, E extends Throwable> Optional<JsonElement> get(DataProvider<T, E> provider) {
        return Optional.ofNullable(data.get("statuses"));
        //return Optional.ofNullable((T) data.get(providr.getClass().getSimpleName()));
    }

    public <T, E extends Throwable> Optional<JsonElement> get(Class<? extends DataProvider<T, E>> clazz) {
        return Optional.ofNullable(data.get("statuses"));
        //return Optional.ofNullable((T) data.get(clazz.getSimpleName()));
    }

    public <T, E extends Throwable> void populate(DataProvider<T, E> provider) throws E {
        T value = provider.provide(profile);
        JsonElement element;
        if (value instanceof JsonElement) {
            element = (JsonElement) value;
        } else {
            element = GSON.toJsonTree(value);
        }
        data.put(provider.getClass().getSimpleName(), element);
    }

    public void clear() {
        data.clear();
    }

    public interface DataProvider<T, E extends Throwable> {
        T provide(User profile) throws E;
    }

    @Override
    public long getId() {
        return profile.getId();
    }

    @Override
    public String getName() {
        return profile.getName();
    }

    @Override
    public String getEmail() {
        return profile.getEmail();
    }

    @Override
    public String getScreenName() {
        return profile.getScreenName();
    }

    @Override
    public String getLocation() {
        return profile.getLocation();
    }

    @Override
    public String getDescription() {
        return profile.getDescription();
    }

    @Override
    public boolean isContributorsEnabled() {
        return profile.isContributorsEnabled();
    }

    @Override
    public String getProfileImageURL() {
        return profile.getProfileImageURL();
    }

    @Override
    public String getBiggerProfileImageURL() {
        return profile.getBiggerProfileImageURL();
    }

    @Override
    public String getMiniProfileImageURL() {
        return profile.getMiniProfileImageURL();
    }

    @Override
    public String getOriginalProfileImageURL() {
        return profile.getOriginalProfileImageURL();
    }

    @Override
    public String getProfileImageURLHttps() {
        return profile.getProfileImageURLHttps();
    }

    @Override
    public String getBiggerProfileImageURLHttps() {
        return profile.getBiggerProfileImageURLHttps();
    }

    @Override
    public String getMiniProfileImageURLHttps() {
        return profile.getMiniProfileImageURLHttps();
    }

    @Override
    public String getOriginalProfileImageURLHttps() {
        return profile.getOriginalProfileImageURLHttps();
    }

    @Override
    public boolean isDefaultProfileImage() {
        return profile.isDefaultProfileImage();
    }

    @Override
    public String getURL() {
        return profile.getURL();
    }

    @Override
    public boolean isProtected() {
        return profile.isProtected();
    }

    @Override
    public int getFollowersCount() {
        return profile.getFollowersCount();
    }

    @Override
    public Status getStatus() {
        return profile.getStatus();
    }

    @Override
    public String getProfileBackgroundColor() {
        return profile.getProfileBackgroundColor();
    }

    @Override
    public String getProfileTextColor() {
        return profile.getProfileTextColor();
    }

    @Override
    public String getProfileLinkColor() {
        return profile.getProfileLinkColor();
    }

    @Override
    public String getProfileSidebarFillColor() {
        return profile.getProfileSidebarFillColor();
    }

    @Override
    public String getProfileSidebarBorderColor() {
        return profile.getProfileSidebarBorderColor();
    }

    @Override
    public boolean isProfileUseBackgroundImage() {
        return profile.isProfileUseBackgroundImage();
    }

    @Override
    public boolean isDefaultProfile() {
        return profile.isDefaultProfile();
    }

    @Override
    public boolean isShowAllInlineMedia() {
        return profile.isShowAllInlineMedia();
    }

    @Override
    public int getFriendsCount() {
        return profile.getFriendsCount();
    }

    @Override
    public Date getCreatedAt() {
        return profile.getCreatedAt();
    }

    @Override
    public int getFavouritesCount() {
        return profile.getFavouritesCount();
    }

    @Override
    public int getUtcOffset() {
        return profile.getUtcOffset();
    }

    @Override
    public String getTimeZone() {
        return profile.getTimeZone();
    }

    @Override
    public String getProfileBackgroundImageURL() {
        return profile.getProfileBackgroundImageURL();
    }

    @Override
    public String getProfileBackgroundImageUrlHttps() {
        return profile.getProfileBackgroundImageUrlHttps();
    }

    @Override
    public String getProfileBannerURL() {
        return profile.getProfileBannerURL();
    }

    @Override
    public String getProfileBannerRetinaURL() {
        return profile.getProfileBannerRetinaURL();
    }

    @Override
    public String getProfileBannerIPadURL() {
        return profile.getProfileBannerIPadURL();
    }

    @Override
    public String getProfileBannerIPadRetinaURL() {
        return profile.getProfileBannerIPadRetinaURL();
    }

    @Override
    public String getProfileBannerMobileURL() {
        return profile.getProfileBannerMobileURL();
    }

    @Override
    public String getProfileBannerMobileRetinaURL() {
        return profile.getProfileBannerMobileRetinaURL();
    }

    @Override
    public boolean isProfileBackgroundTiled() {
        return profile.isProfileBackgroundTiled();
    }

    @Override
    public String getLang() {
        return profile.getLang();
    }

    @Override
    public int getStatusesCount() {
        return profile.getStatusesCount();
    }

    @Override
    public boolean isGeoEnabled() {
        return profile.isGeoEnabled();
    }

    @Override
    public boolean isVerified() {
        return profile.isVerified();
    }

    @Override
    public boolean isTranslator() {
        return profile.isTranslator();
    }

    @Override
    public int getListedCount() {
        return profile.getListedCount();
    }

    @Override
    public boolean isFollowRequestSent() {
        return profile.isFollowRequestSent();
    }

    @Override
    public URLEntity[] getDescriptionURLEntities() {
        return profile.getDescriptionURLEntities();
    }

    @Override
    public URLEntity getURLEntity() {
        return profile.getURLEntity();
    }

    @Override
    public String[] getWithheldInCountries() {
        return profile.getWithheldInCountries();
    }

    @Override
    public int compareTo(@NotNull User o) {
        return profile.compareTo(o);
    }

    @Override
    public RateLimitStatus getRateLimitStatus() {
        throw new RuntimeException("Can't be accessed in this context");
    }

    @Override
    public int getAccessLevel() {
        return profile.getAccessLevel();
    }
}
