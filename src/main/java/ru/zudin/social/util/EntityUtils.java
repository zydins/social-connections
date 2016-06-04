package ru.zudin.social.util;

import org.apache.hadoop.io.Text;
import ru.zudin.social.model.SocialUser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * @author sergey
 * @since 04.06.16
 */
public class EntityUtils {

    public static final String DEFAULT_VALUE = "-";

    public static <T extends SocialUser> void readProperty(DataInput in, String firstName, T obj)
            throws IOException, NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getField(firstName);
        Text text = new Text();
        text.readFields(in);
        String value = text.toString();
        if (!value.equals(DEFAULT_VALUE)) {
            field.set(obj, value);
        }
    }

    public static void writeProperty(DataOutput out, String param) throws IOException {
        new Text(defaultIfNull(param, DEFAULT_VALUE)).write(out);
    }

}
