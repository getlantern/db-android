package io.lantern.db

import org.junit.Assert
import org.junit.Test
import java.nio.charset.Charset

class RawTest {
    @Test
    fun testRaw() {
        val testString = "text"
        val pbufMessage =
            io.lantern.db.Test.TestMessage.newBuilder().setName("name").setNumber(5).build()
        val jsonMessage = Message(name = "hi", number = 1)

        val serde = Serde()
        serde.register(20, pbufMessage.javaClass)
        serde.register(21, Message::class.java)

        val stringRaw = Raw(serde, testString)
        val pbufRaw = Raw(serde, pbufMessage)
        val jsonRaw = Raw(serde, jsonMessage)
        val kryoRaw = Raw(serde, 10)

        Assert.assertEquals(testString, stringRaw.value)
        Assert.assertEquals(testString, stringRaw.valueOrProtoBytes)
        Assert.assertEquals(
            testString,
            stringRaw.bytes.toString(Charset.defaultCharset())
        )

        Assert.assertEquals(pbufMessage, pbufRaw.value)
        val pbufSerialized = serde.serialize(pbufMessage)
        Assert.assertTrue(
            pbufSerialized.copyOfRange(3, pbufSerialized.size)
                .contentEquals(pbufRaw.valueOrProtoBytes as ByteArray)
        )
        Assert.assertTrue(
            pbufSerialized.copyOfRange(3, pbufSerialized.size).contentEquals(pbufRaw.bytes)
        )

        Assert.assertEquals(jsonMessage, jsonRaw.value)
        val jsonSerialized = serde.serialize(pbufMessage)
        Assert.assertTrue(
            jsonSerialized.copyOfRange(3, jsonSerialized.size)
                .contentEquals(pbufRaw.valueOrProtoBytes as ByteArray)
        )
        Assert.assertTrue(
            jsonSerialized.copyOfRange(3, jsonSerialized.size).contentEquals(pbufRaw.bytes)
        )

        Assert.assertEquals(10, kryoRaw.value)
        try {
            kryoRaw.bytes
            Assert.fail("attempting to get bytes from Kryo raw should have thrown exception")
        } catch (e: AssertionError) {
            // expected
        }
    }
}
