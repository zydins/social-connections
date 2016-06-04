package ru.zudin.social.model;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static ru.zudin.social.util.EntityUtils.readProperty;
import static ru.zudin.social.util.EntityUtils.writeProperty;

/**
 * @author sergey
 * @since 01.06.16
 */
public class VKUser implements SocialUser {

    public int userId;
    public String firstName;
    public String lastName;
    public String domain;
    public String nickname;

    public String skype;
    public String facebook;
    public String facebookName;
    public String twitter;
    public String instagram;
    public String livejournal;

    @Override
    public long getId() {
        return userId;
    }

    @Override
    public List<String> getNames() {
        List<String> strings = Arrays.asList(livejournal, instagram, twitter, facebookName, skype,
                domain, firstName + " " + lastName, nickname);
        return strings.stream()
                .distinct()
                .collect(toList());
    }

    @Override
    public String getEntityName() {
        return "VK";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        writeProperty(out, firstName);
        writeProperty(out, lastName);
        writeProperty(out, domain);
        writeProperty(out, nickname);
        writeProperty(out, skype);
        writeProperty(out, facebook);
        writeProperty(out, facebookName);
        writeProperty(out, twitter);
        writeProperty(out, instagram);
        writeProperty(out, livejournal);
    }

    

    @Override
    public void readFields(DataInput in) throws IOException, IllegalStateException {
        try {
            userId = in.readInt();
            readProperty(in, "firstName", this);
            readProperty(in, "lastName", this);
            readProperty(in, "domain", this);
            readProperty(in, "nickname", this);
            readProperty(in, "skype", this);
            readProperty(in, "facebook", this);
            readProperty(in, "facebookName", this);
            readProperty(in, "twitter", this);
            readProperty(in, "instagram", this);
            readProperty(in, "livejournal", this);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    
}
