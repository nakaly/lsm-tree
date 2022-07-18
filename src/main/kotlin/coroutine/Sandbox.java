package coroutine;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sandbox {

    public static void main (String... args) {
        System.out.println(getEnums(Hoge.class, "A B"));
    }


    public static <T extends Enum<T>> List<T> getEnums(Class<T> enumClass, String line) {
        return Stream.of(line.split(" ")).map(item -> getEnum(enumClass, item).get()).collect(Collectors.toList());
    }

    public static <T extends Enum<T>> Optional<T> getEnum(Class<T> enumClass, String code) {
        return EnumSet.allOf(enumClass).stream().filter(e -> e.name().equals(code)).findFirst();
    }

    enum Hoge {
        A,
        B,
    }
}
