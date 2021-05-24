package lsm

import lsm.sstable.SSTableFactory
import lsm.sstable.SegmentFileReadable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.test.assertContentEquals

class SSTableFactoryTest {

    val classLoader = javaClass.classLoader

    val expectedSSTableBinary by lazy {
        val readAllBytes = Files.readAllBytes(
            Paths.get(
                classLoader
                    .getResource("test/sstable/segment_file_1_test.txt")
                    .toURI()
            )
        )
        readAllBytes
    }

    val currentSequenceNo = 1
    val testResourceDirectory = classLoader.getResource("test/sstable").path
    val sSTableFactory = SSTableFactory(3, testResourceDirectory)

    @AfterEach
    fun clean() {
        val file = File(
            classLoader
                .getResource("test/sstable")
                .getPath()
                    + "/segment_file_$currentSequenceNo.txt"
        )
        if (file.exists()) {
            file.delete()
        }
    }

    @Test
    fun recoverAndApply() {
        val writeIterator =
            mutableMapOf(
                "bar" to SegmentFileReadable.DeletedValue,
                "baz" to SegmentFileReadable.Exist("bazValue"),
                "foo" to SegmentFileReadable.Exist("fooValue"),
                "key1" to SegmentFileReadable.DeletedValue,
                "key2" to SegmentFileReadable.Exist("0040999494"),
                "key3" to SegmentFileReadable.Exist("93948848"),
                "key4" to SegmentFileReadable.Exist("l30ls@@zsds")
            )
                .entries.iterator()

        val sSTable = sSTableFactory.apply(currentSequenceNo, writeIterator)

        val expectedSparseKeys = arrayOf("bar", "key1", "key4")
        val expectedPositions = arrayOf(0L, 49L, 103L)
        assertContentEquals(sSTable.sparseKeyIndex.sparseKeys, expectedSparseKeys)
        assertContentEquals(sSTable.sparseKeyIndex.keyPositions, expectedPositions)

        val sSTableBinary =
            Files.readAllBytes(
                Paths.get(
                    classLoader.getResource("test/sstable/segment_file_1.txt").toURI()
                )
            )
        assertContentEquals(sSTableBinary, expectedSSTableBinary)

        val recoveredSSTable = sSTableFactory.recovery(currentSequenceNo)
        assertContentEquals(sSTable.sparseKeyIndex.sparseKeys, recoveredSSTable.sparseKeyIndex.sparseKeys)
        assertContentEquals(sSTable.sparseKeyIndex.keyPositions, recoveredSSTable.sparseKeyIndex.keyPositions)


    }


}