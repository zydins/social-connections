package ru.zudin.social.model;

/**
 * @author sergey
 * @since 06.04.17
 */
public class SocialName {

    public String name;
    public String value;
    public SocialNetwork network;

    public SocialName(String name, SocialNetwork network) {
        this.network = network;
        this.name = name;
    }

    public SocialName(String name, String value, SocialNetwork network) {
        this.name = name;
        this.value = value;
        this.network = network;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SocialName that = (SocialName) o;

        if (!name.equals(that.name)) return false;
        return network == that.network;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + network.hashCode();
        return result;
    }

}
