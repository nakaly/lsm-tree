package lsm

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SparseKeyIndexTest {

    val index = SparseKeyIndex(
        arrayOf("bar", "baz", "foo", "key1", "key2", "key3", "key4"),
        arrayOf(84L, 95L, 194L, 345L, 442L, 809L, 1204L)
    )

    @Test
    fun not_found() {
        assertEquals(index.positionRange("arr"), SparseKeyIndex.NotFound)
    }

    @Test
    fun found() {
        assertEquals(index.positionRange("foo"), SparseKeyIndex.Found(194L))
        assertEquals(index.positionRange("bar"), SparseKeyIndex.Found(84L))
        assertEquals(index.positionRange("key4"), SparseKeyIndex.Found(1204L))
    }

    @Test
    fun range() {
        assertEquals(index.positionRange("car"), SparseKeyIndex.Range(95L, 194L))
        assertEquals(index.positionRange("key1z"), SparseKeyIndex.Range(345L, 442L))
    }

    @Test
    fun tail() {
        assertEquals(index.positionRange("zey"), SparseKeyIndex.Tail(1204L))
        assertEquals(index.positionRange("key4a"), SparseKeyIndex.Tail(1204L))
    }

}