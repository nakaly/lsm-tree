package lsm

import lsm.sstable.Log
import lsm.sstable.Logs
import lsm.sstable.SSTable
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.kotlin.mock
import java.util.*
import kotlin.test.assertContentEquals

class LogsTest {


    @Test
    fun activeSequenceNo() {
        val memTable1 = mock<Log.MemTable>()
        val memTable2 = mock<Log.MemTable>()

        val sSTable1 = mock<SSTable>()
        `when`(sSTable1.sequenceNo).thenReturn(2)
        val sSTableRef1 = mock<Log.SSTableRef>()
        `when`(sSTableRef1.sStable).thenReturn(sSTable1)

        val sSTable2 = mock<SSTable>()
        `when`(sSTable2.sequenceNo).thenReturn(5)
        val sSTableRef2 = mock<Log.SSTableRef>()
        `when`(sSTableRef2.sStable).thenReturn(sSTable2)

        val sSTable3 = mock<SSTable>()
        `when`(sSTable3.sequenceNo).thenReturn(7)
        val sSTableRef3 = mock<Log.SSTableRef>()
        `when`(sSTableRef3.sStable).thenReturn(sSTable3)

        val sSTable4 = mock<SSTable>()
        `when`(sSTable4.sequenceNo).thenReturn(9)
        val sSTableRef4 = mock<Log.SSTableRef>()
        `when`(sSTableRef4.sStable).thenReturn(sSTable4)


        val map = TreeMap<Int, String>(Comparator.reverseOrder())
        map.set(1, "test")
        map.set(2, "test2")
        map.values.forEach { println(it) }


        val logs = Logs()
            .updated(2, sSTableRef1)
            .updated(5, sSTableRef2)
            .updated(7, sSTableRef3)
            .updated(8, memTable1)
            .updated(9, sSTableRef4)
            .updated(13, memTable2)

        val activeSequenceNo = logs.activeSequenceNo()
        activeSequenceNo.forEach { println(it) }
        assertContentEquals(activeSequenceNo, sequenceOf(9, 7, 5, 2))
    }
}