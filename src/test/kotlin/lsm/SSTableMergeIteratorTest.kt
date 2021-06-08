package lsm

import lsm.sstable.SSTableFactory
import lsm.sstable.SSTableMergeIterator
import lsm.sstable.SegmentFileReadable
import kotlin.test.Test
import kotlin.test.assertEquals

class SSTableMergeIteratorTest {

    private val classLoader = javaClass.classLoader

    val nextSequenceNo = 5
    val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath()
    val sSTableFactory = SSTableFactory(100, testResourceDirectory)

    val sSTable2 = sSTableFactory.recovery(2)
    val sSTable3 = sSTableFactory.recovery(3)
    val sSTable4 = sSTableFactory.recovery(4)

    @Test
    fun merge() {
        val merged = SSTableMergeIterator(listOf(sSTable2, sSTable3, sSTable4)).asSequence().toList()
        val expected = listOf<Pair<String, SegmentFileReadable.Value>>(
            Pair("bar", SegmentFileReadable.DeletedValue),
            Pair("baz", SegmentFileReadable.Exist("bazValue")),
            Pair("foo", SegmentFileReadable.Exist("fooValue")),
            Pair("foo_", SegmentFileReadable.Exist("fooVa?")),
            Pair("key1", SegmentFileReadable.DeletedValue),
            Pair("key1_2", SegmentFileReadable.DeletedValue),
            Pair("key2", SegmentFileReadable.Exist("0040999494")),
            Pair("key2_1", SegmentFileReadable.Exist("efffsd")),
            Pair("key3", SegmentFileReadable.Exist("93948848")),
            Pair("key_3", SegmentFileReadable.Exist("value_V3")),
            Pair("keay4", SegmentFileReadable.Exist("l3dd0ls@@zsds")),
            Pair("key4", SegmentFileReadable.Exist("l30ls@@zsds")),
            Pair("key4.5", SegmentFileReadable.Exist("dddd"))
        ).sortedBy { it.first }
        assertEquals(expected, merged)


    }
}