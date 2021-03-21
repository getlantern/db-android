package io.lantern.db

import org.junit.Assert
import org.junit.Test
import java.nio.charset.Charset
import java.util.*

class RawTest {
    @Test
    fun testRaw() {
        val testString = "text"
        val testMessage =
            io.lantern.db.Test.TestMessage.newBuilder().setName("name").setNumber(5).build()

        val serde = Serde()
        serde.register(20, testMessage.javaClass)

        val stringRaw = Raw(serde, testString)
        val pbufRaw = Raw(serde, testMessage)
        val kryoRaw = Raw(serde, 10)

        Assert.assertEquals(testString, stringRaw.value)
        Assert.assertEquals(
            testString,
            stringRaw.bytes.toString(Charset.defaultCharset())
        )

        Assert.assertEquals(testMessage, pbufRaw.value)
        val serialized = serde.serialize(testMessage)
        Assert.assertTrue(
            Arrays.equals(
                serialized.copyOfRange(3, serialized.size),
                pbufRaw.bytes
            )
        )

        Assert.assertEquals(10, kryoRaw.value)
        try {
            kryoRaw.bytes
            Assert.fail("attempting to get bytes from Kryo raw should have thrown exception")
        } catch (t: Throwable) {
            // expected
        }
    }
}