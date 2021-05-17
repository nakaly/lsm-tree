package SimpleKVS

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File


class SimpleKVSTest {

    @Test
    fun set_get() {
        val kvs = SimpleKVS("data/simplekvs/test_database.txt")
        val key = "key"
        val value = "value"
        kvs.set(key, value)
        val readValue = kvs.get(key)
        assertEquals(readValue, value)
        File("data/simplekvs/test_database.txt").delete()
    }

    @Test
    fun replace_value_by_two_set() {
        val kvs = SimpleKVS("data/simplekvs/test_database.txt")
        val key = "key"
        val value = "value"
        kvs.set(key, value)
        val value2 = "value2"
        kvs.set(key, value2)
        val readValue = kvs.get(key)
        assertEquals(readValue, value2)
        File("data/simplekvs/test_database.txt").delete()
    }

    @Test
    fun del() {
        val kvs = SimpleKVS("data/simplekvs/test_database.txt")
        val key = "key"
        val value = "value"
        kvs.set(key, value)
        kvs.del(key)
        val readValue = kvs.get(key)
        assertEquals(readValue, null)
        File("data/simplekvs/test_database.txt").delete()
    }

}