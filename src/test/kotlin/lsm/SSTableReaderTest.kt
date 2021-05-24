package lsm

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SSTableReaderTest {

    val classLoader = javaClass.classLoader
    private val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath()
    private val sSTableFactory = SSTableFactory(2, testResourceDirectory)
    private val sSTable = sSTableFactory.recovery(4)
    val reader: SSTable.SSTableReader = sSTable.newReader()

    @AfterAll
    fun after() {
        reader.close()
    }

    @Test
    fun found() {
        assertEquals(reader.get("foo"), SegmentFileReadable.Found("fooValue"))
        assertEquals(reader.get("key2"), SegmentFileReadable.Found("0040999494"))
    }

    @Test
    fun found_without_index() {
        assertEquals(reader.get("baz"), SegmentFileReadable.Found("bazValue"))
        assertEquals(reader.get("key3"), SegmentFileReadable.Found("93948848"))
    }

    @Test
    fun not_found() {
        assertEquals(reader.get("aar"), SegmentFileReadable.NotFound)
        assertEquals(reader.get("car"), SegmentFileReadable.NotFound)
        assertEquals(reader.get("xxr"), SegmentFileReadable.NotFound)
    }

    @Test
    fun deleted() {
        assertEquals(reader.get("bar"), SegmentFileReadable.Deleted)
        assertEquals(reader.get("key1"), SegmentFileReadable.Deleted)
    }
}