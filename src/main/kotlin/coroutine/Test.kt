package coroutine

import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

class Test {

    object Sandbox {
        @JvmStatic
        fun main(args: Array<String>) {
            println(getEnums(Hoge::class.java, "A B"))
        }

        fun <T : Enum<T>?> getEnums(enumClass: Class<T>?, line: String): List<T> {
            return Stream.of(*line.split(" ").toTypedArray()).map { item: String -> getEnum(enumClass, item).get() }.collect(Collectors.toList())
        }

        fun <T : Enum<T>?> getEnum(enumClass: Class<T>?, code: String): Optional<T> {
            return EnumSet.allOf(enumClass).stream().filter { e: T -> e!!.name == code }.findFirst()
        }

        internal enum class Hoge {
            A, B
        }
    }
}