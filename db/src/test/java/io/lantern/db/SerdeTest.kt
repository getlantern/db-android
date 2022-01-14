package io.lantern.db

import io.lantern.db.Test.TestMessage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.nio.charset.Charset

class SerdeTest {
    @Test
    fun testString() {
        val serde = Serde()
        val bytes = serde.serialize("brian")
        assertEquals(
            "string should be serialized in optimized text format",
            "Tbrian",
            bytes.toString(Charset.defaultCharset())
        )
        val str = serde.deserialize<String>(bytes)
        assertEquals("round-tripped value should match original", "brian", str)
    }

    @Test
    fun testKryoWithAndWithoutRegistration() {
        val serde = Serde()
        val obj = Message("name", 100)
        val bytesUnregistered = serde.serializeLegacyMode(obj)
        assertEquals(
            "unregistered object type should be serialized with kryo",
            'K',
            bytesUnregistered[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped unregistered kryo object should match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )

        try {
            serde.register(19, Message::class.java)
            fail("registering an ID below 20 should have failed")
        } catch (e: IllegalArgumentException) {
            // expected
        }
        serde.register(20, Message::class.java)
        val bytesRegistered = serde.serializeLegacyMode(obj)
        assertEquals(
            "registered object type should be serialized with kryo",
            'K',
            bytesRegistered[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped registered kryo object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
        assertEquals(
            "round-tripped unregistered kryo object should still match original",
            obj,
            serde.deserialize(bytesUnregistered)
        )
        assertTrue(
            "serialized form of registered object should be shorter than unregistered",
            bytesRegistered.size < bytesUnregistered.size
        )
    }

    @Test
    fun testProtocolBufferObjectWithAndWithoutRegistration() {
        val serde = Serde()
        val obj = Message("name", 100)
        try {
            serde.serialize(obj)
            fail("attempt to serialize unregistered protocol buffer object should fail")
        } catch (e: AssertionError) {
            // expected
        }

        try {
            serde.register(19, Message::class.java)
            fail("registering an ID below 20 should have failed")
        } catch (e: IllegalArgumentException) {
            // expected
        }
        serde.register(20, Message::class.java)
        val bytesRegistered = serde.serialize(obj)
        assertEquals(
            "registered object type should be serialized with JSON",
            'J',
            bytesRegistered[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped registered JSON object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
    }

    @Test
    fun testJsonObjectWithAndWithoutRegistration() {
        val serde = Serde()
        val obj = TestMessage.newBuilder().setName("name").setNumber(100).build()
        try {
            serde.serialize(obj)
            fail("attempt to serialize unregistered object should fail")
        } catch (e: AssertionError) {
            // expected
        }

        try {
            serde.register(19, TestMessage::class.java)
            fail("registering an ID below 20 should have failed")
        } catch (e: IllegalArgumentException) {
            // expected
        }
        serde.register(20, TestMessage::class.java)
        val bytesRegistered = serde.serialize(obj)
        assertEquals(
            "registered object type should be serialized with protocol buffers",
            'P',
            bytesRegistered[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped registered protocol buffer object should match original",
            obj,
            serde.deserialize(bytesRegistered)
        )
    }

    @Test
    fun testMigration() {
        val serde = Serde()

        val legacyBytes = serde.serializeLegacyMode(5)
        assertEquals(
            "legacy serialization should have used Kryo",
            'K',
            legacyBytes[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped legacy value should match original",
            5,
            serde.deserialize<Int>(legacyBytes)
        )

        val newBytes = serde.serialize(5)
        assertEquals(
            "new style serialization should have serialized integer",
            'I',
            newBytes[0].toInt().toChar()
        )
        assertEquals(
            "round-tripped new style value should match original",
            5,
            serde.deserialize<Int>(newBytes)
        )
    }
}

internal data class Message(val name: String = "", val number: Int = 0)
