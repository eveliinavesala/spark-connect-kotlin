package dataset.java_api;

import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {
    private String name;
    private int age;
    private int cityId;

    public Person() {}

    public Person(String name, int age) {
        this(name, age, 0);
    }

    public Person(String name, int age, int cityId) {
        this.name = name;
        this.age = age;
        this.cityId = cityId;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }
    public int getCityId() { return cityId; }
    public void setCityId(int cityId) { this.cityId = cityId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return age == person.age && cityId == person.cityId && Objects.equals(name, person.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age, cityId);
    }
}
