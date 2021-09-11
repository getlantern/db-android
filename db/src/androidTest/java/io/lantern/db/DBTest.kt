/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */
package io.lantern.db

import android.content.Context
import android.content.SharedPreferences
import androidx.core.content.edit
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.util.HashMap
import java.util.Random

@RunWith(AndroidJUnit4::class)
class DBTest {
    private var tempDir: File? = null

    @Test
    fun testQuery() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.putAll(
                    mapOf(
                        "/contacts/32af234asdf324" to "That Person",
                        "/contacts/32af234asdf324/messages_by_timestamp/1" to "/messages/c",
                        "/contacts/32af234asdf324/messages_by_timestamp/2" to "/messages/a",
                        "/contacts/32af234asdf324/messages_by_timestamp/3" to "/messages/b",
                        "/contacts/32af234asdf324/messages_by_timestamp/4" to "/messages/e", // this one doesn't exist
                        "/messages/c" to "Message C",
                        "/messages/d" to "Message D", // this one isn't referenced by messages_by_timestamp
                        "/messages/a" to "Message A",
                        "/messages/b" to "Message B",
                    )
                )
            }
            assertEquals("That Person", db.get("/contacts/32af234asdf324"))
            val raw = db.getRaw<String>("/contacts/32af234asdf324")
            assertEquals(
                Raw(db.serde, "That Person"),
                raw
            )

            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/a", "Message A"),
                    PathAndValue("/messages/b", "Message B"),
                    PathAndValue("/messages/c", "Message C"),
                    PathAndValue("/messages/d", "Message D")
                ),
                db.list<String>("/messages/%")
            )
            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/a", Raw(db.serde, "Message A")),
                    PathAndValue("/messages/b", Raw(db.serde, "Message B")),
                    PathAndValue("/messages/c", Raw(db.serde, "Message C")),
                    PathAndValue("/messages/d", Raw(db.serde, "Message D"))
                ),
                db.listRaw<String>("/messages/%")
            )

            assertEquals(
                arrayListOf(
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/3",
                        "/messages/b",
                        "Message B"
                    ),
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/2",
                        "/messages/a",
                        "Message A"
                    ),
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/1",
                        "/messages/c",
                        "Message C"
                    ),
                ),
                db.listDetails<String>(
                    "/contacts/32af234asdf324/messages_by_timestamp/%",
                    0,
                    10,
                    reverseSort = true
                )
            )

            assertEquals(
                arrayListOf(
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/2",
                        "/messages/a",
                        "Message A"
                    ),
                ),
                db.listDetails<String>(
                    "/contacts/%/messages_by_timestamp/2",
                    0,
                    10,
                    reverseSort = true
                )
            )

            assertEquals(
                arrayListOf(
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/3",
                        "/messages/b",
                        Raw(db.serde, "Message B")
                    ),
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/2",
                        "/messages/a",
                        Raw(db.serde, "Message A")
                    ),
                    Detail(
                        "/contacts/32af234asdf324/messages_by_timestamp/1",
                        "/messages/c",
                        Raw(db.serde, "Message C")
                    ),
                ),
                db.listDetailsRaw<String>(
                    "/contacts/32af234asdf324/messages_by_timestamp/%",
                    0,
                    10,
                    reverseSort = true
                )
            )

            assertEquals(
                arrayListOf("Message C", "Message A", "Message B"),
                db.listDetails<String>("/contacts/32af234asdf324/messages_by_timestamp/%", 0, 10)
                    .map { it.value }
            )
            assertEquals(
                arrayListOf("Message A"),
                db.listDetails<String>("/contacts/32af234asdf324/messages_by_timestamp/%", 1, 1)
                    .map { it.value }
            )
            assertEquals(
                arrayListOf("Message B"),
                db.list<String>("/messages/%", 1, 1)
                    .map { it.value }
            )
            assertEquals(
                arrayListOf("/messages/b"),
                db.listPaths("/messages/%", 1, 1)
            )
            assertEquals(
                0,
                db.listDetails<String>(
                    "/contacts/32af234asdf324/messages_by_timestamp/%",
                    3,
                    10
                ).size
            )
        }
    }

    @Test
    fun testFullTextQuery() {
        val values = mapOf(
            "/messages/c" to Message("Message C blah blah"),
            "/messages/d" to Message("Message D blah blah blah"),
            "/messages/a" to Message("Message A blah"),
            "/messages/b" to Message("Message B"),
        )
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                values.forEach { (path, value) ->
                    tx.put(path, value, fullText = value.body)
                }
                tx.putAll(
                    mapOf(
                        "/list/1" to "/messages/a",
                        "/list/2" to "/messages/b",
                        "/list/3" to "/messages/c",
                        "/list/4" to "/messages/d",
                    )
                )
            }

            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/a", Message("Message A blah")),
                    PathAndValue("/messages/b", Message("Message B")),
                    PathAndValue("/messages/c", Message("Message C blah blah")),
                    PathAndValue("/messages/d", Message("Message D blah blah blah"))
                ),
                db.list<Message>("/messages/%")
            )

            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/d", Message("Message D blah blah blah")),
                    PathAndValue("/messages/c", Message("Message C blah blah")),
                    PathAndValue("/messages/a", Message("Message A blah"))
                ),
                db.list<Message>("/%", fullTextSearch = "blah")
            )

            assertEquals(
                arrayListOf(
                    Detail("/list/4", "/messages/d", Message("Message D blah blah blah")),
                    Detail("/list/3", "/messages/c", Message("Message C blah blah")),
                    Detail("/list/1", "/messages/a", Message("Message A blah"))
                ),
                db.listDetails<Message>("/list/%", fullTextSearch = "blah")
            )

            // test prefix match with highlighting
            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/d", Message("Message D *bla*h *bla*h *bla*h")),
                    PathAndValue("/messages/c", Message("Message C *bla*h *bla*h")),
                    PathAndValue("/messages/a", Message("Message A *bla*h"))
                ),
                db.list<Message>("/%", fullTextSearch = "bla*") { msg, highlight ->
                    Message(highlight(msg.body))
                }
            )

            // now delete
            db.mutatePublishBlocking { tx ->
                // delete an entry including the full text index
                tx.delete("/messages/d")
                // add the entry back without full-text indexing to make sure it doesn't show up in results
                tx.put("/messages/d", Message("Message D blah blah blah"))
                // delete another entry without deleting the full text index
                tx.delete("/messages/c")
            }

            assertEquals(
                arrayListOf(
                    PathAndValue("/messages/a", Message("Message A blah"))
                ),
                db.list<Message>("/%", fullTextSearch = "blah")
            )
        }
    }

    @Test
    fun testPut() {
        buildDB().use { db ->
            val updates = ArrayList<String>()
            db.subscribe(object : Subscriber<String>("100", "path") {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.values.forEach { updates.add(it) }
                }
            })

            db.mutatePublishBlocking { tx ->
                assertTrue(tx.putIfAbsent("path", "a"))
            }
            assertEquals("correct value should have been inserted", "a", db.get("path"))

            db.mutatePublishBlocking { tx ->
                assertFalse(tx.putIfAbsent("path", "b"))
            }
            assertEquals(
                "value should not have been udpated by putIfAbsent",
                "a",
                db.get("path")
            )

            val putValue = db.mutatePublishBlocking { tx ->
                tx.put("path", "c")
            }
            assertEquals("put should have returned correct value", "c", putValue)
            assertEquals("value should have been udpated by regular put", "c", db.get("path"))

            assertEquals(arrayListOf("a", "c"), updates)

            db.mutate { tx ->
                tx.put("ap", "a")
                assertEquals("a", tx.get("ap"))
                db.mutate { tx2 ->
                    assertEquals("a", tx2.get("ap"))
                    assertEquals("a", db.get("ap"))
                }
            }
        }
    }

    fun testGetOrPut() {
        buildDB().use { db ->
            var putValue = db.mutatePublishBlocking { tx ->
                tx.getOrPut("path", "a")
            }
            assertEquals("new value should have been returned by getOrPut", "a", putValue)
            assertEquals("correct value should have been inserted", "a", db.get("path"))

            putValue = db.mutatePublishBlocking { tx ->
                tx.getOrPut("path", "b")
            }
            assertEquals("old value should have been returned by getOrPut", "a", putValue)
            assertEquals("existing value should have been left alone", "a", db.get("path"))
        }
    }

    @Test
    fun testGetDetails() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.put("/detail", "detail")
                tx.put("/index", "/detail")
            }

            assertEquals("detail", db.getDetail("/index"))
        }
    }

    @Test
    fun testFindOne() {
        buildDB().use { db ->
            val query = "/path/%/thing"
            assertNull(db.findOne(query))
            db.mutatePublishBlocking { tx ->
                tx.put("/path/1/thing", "1")
            }
            assertEquals("1", db.findOne(query))
            assertEquals("/path/1/thing", db.findOneRaw<String>(query)?.path)
            db.mutatePublishBlocking { tx ->
                tx.put("/path/2/thing", "2")
            }
            try {
                db.findOne<Any>(query)
                fail("findOne should have failed when 2 values match")
            } catch (t: Throwable) {
                assertTrue(t is TooManyMatchesException)
            }
        }
    }

    @Test
    fun testSubscribeDirectLate() {
        buildDB().use { db ->
            var theValue = "the value"

            db.mutatePublishBlocking { tx ->
                tx.put("path", theValue)
            }
            db.subscribe(object : Subscriber<String>(
                "100",
                // note the use of a gratuitous trailing % which will be ignored
                "path%"
            ) {
                    override fun onInitial(values: List<PathAndValue<Raw<String>>>) {
                        assertEquals(1, values.size)
                        assertEquals("path", values[0].path)
                        assertEquals("the value", values[0].value.value)
                    }

                    override fun onChanges(changes: ChangeSet<String>) {
                        changes.updates.forEach { (path, value) ->
                            assertEquals("path", path)
                            assertEquals("new value", value)
                        }
                    }
                })

            theValue = "new value"
            db.mutatePublishBlocking { tx ->
                tx.put("path", theValue)
            }
        }
    }

    @Test
    fun testSubscribeDirect() {
        buildDB().use { db ->
            var currentValue = "original value"

            db.subscribe(object : Subscriber<String>("100", "path") {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.forEach { _ ->
                        fail("this subscriber was replaced and should never have been notified")
                    }
                }
            })

            try {
                db.subscribe(object : Subscriber<String>("100", "path") {
                    override fun onChanges(changes: ChangeSet<String>) {}
                })
                fail("re-registering already registered subscriber ID should not be allowed")
            } catch (t: IllegalArgumentException) {
                // expected
            }

            db.unsubscribe("100")

            db.subscribe(object : Subscriber<String>("100", "path") {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.forEach { (path, value) ->
                        assertEquals("path", path)
                        assertEquals("original value", value)
                    }
                }
            })

            var calledDelete = false

            db.subscribe(object : Subscriber<String>("101", "path") {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.forEach { (path, value) ->
                        assertEquals("path", path)
                        assertEquals(currentValue, value)
                    }
                    changes.deletions.forEach { _ -> calledDelete = true }
                }
            })

            db.unsubscribe("100")

            db.mutatePublishBlocking { tx ->
                tx.put("path", currentValue)
            }
            currentValue = "new value"
            db.mutatePublishBlocking { tx ->
                tx.put("path", currentValue)
            }

            db.mutatePublishBlocking { tx -> tx.delete("path") }
            assertTrue(calledDelete)
        }
    }

    @Test
    fun testSubscribePrefixNoInit() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.putAll(
                    mapOf(
                        "/path/1" to "1",
                        "/path/2" to "2",
                    )
                )
            }

            var updateCalled = false
            var deleteCalled = false
            db.subscribe(
                object : Subscriber<String>("100", "/path") {
                    override fun onChanges(changes: ChangeSet<String>) {
                        changes.updates.forEach { (path, value) ->
                            assertEquals("/path/3", path)
                            assertEquals("3", value)
                            updateCalled = true
                        }
                        changes.deletions.forEach { _ -> deleteCalled = true }
                    }
                },
                receiveInitial = false
            )
            db.subscribe(
                object : Subscriber<String>("101", "/pa/") {
                    override fun onChanges(changes: ChangeSet<String>) {
                        changes.updates.forEach { _ ->
                            fail("subscriber with no common prefix shouldn't get updates")
                        }
                        changes.deletions.forEach { _ ->
                            fail("subscriber with no common prefix shouldn't get deletions")
                        }
                    }
                },
                receiveInitial = false
            )

            db.mutatePublishBlocking { tx ->
                tx.delete("/path/1")
                tx.put("/path/3", "3")
            }

            assertTrue(updateCalled)
            assertTrue(deleteCalled)
        }
    }

    @Test
    fun testSubscribePrefixInit() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.putAll(
                    mapOf(
                        "/path/1" to "1",
                        "/path/2" to "2",
                        "/pa/1" to "1",
                    )
                )
            }

            var updates = 0
            db.subscribe(object : Subscriber<String>("100", "/path") {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.forEach { (path, value) ->
                        assertTrue(path.startsWith("/path/"))
                        assertEquals(path.substring(path.length - 1), value)
                        updates += 1
                    }
                }
            })

            assertEquals(2, updates)
        }
    }

    @Test
    fun testSubscribeDetailsPrefixInit() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.putAll(
                    mapOf(
                        "/detail/1" to "1",
                        "/detail/2" to "2",
                        "/list/1" to "/detail/2",
                        "/list/2" to "/detail/1"
                    )
                )
            }

            val updates = HashSet<PathAndValue<PathAndValue<String>>>()
            val deletions = HashSet<String>()

            db.subscribe(object : DetailsSubscriber<String>("100", "/list/") {
                override fun onChanges(changes: DetailsChangeSet<String>) {
                    changes.updates.forEach { (path, value) ->
                        updates.add(PathAndValue(path, PathAndValue(value.path, value.value.value)))
                    }

                    changes.deletions.forEach { path -> deletions.add(path) }
                }
            })

            db.mutatePublishBlocking { tx ->
                tx.put("/detail/1", "11")
                tx.put("/list/3", "/detail/3")
                tx.put("/detail/3", "3")
                tx.put(
                    "/list/4",
                    "/detail/unknown"
                ) // since this doesn't have details, we shouldn't get notified about it
                tx.put(
                    "/detail/4",
                    "4"
                ) // since this detail doesn't link back to a path in the list, we shouldn't get notified about it
            }

            db.mutatePublishBlocking { tx ->
                tx.delete("/detail/2") // should show up as a deletion of /list/1
                tx.delete("/list/2")
            }

            assertEquals(
                setOf(
                    PathAndValue("/list/1", PathAndValue("/detail/2", "2")),
                    PathAndValue("/list/2", PathAndValue("/detail/1", "1")),
                    PathAndValue("/list/2", PathAndValue("/detail/1", "11")),
                    PathAndValue("/list/3", PathAndValue("/detail/3", "3")),
                ),
                updates
            )

            assertEquals(setOf("/list/1", "/list/2"), deletions)
        }
    }

    @Test
    fun testSubscribeDetailsWithCompoundChanges() {
        buildDB().use { db ->
            val updates = ArrayList<PathAndValue<PathAndValue<String>>>()
            val deletions = ArrayList<String>()

            db.subscribe(
                object : DetailsSubscriber<String>("100", "/list/") {
                    override fun onChanges(changes: DetailsChangeSet<String>) {
                        changes.updates.forEach { (path, value) ->
                            updates.add(PathAndValue(path, PathAndValue(value.path, value.value.value)))
                        }

                        changes.deletions.forEach { path -> deletions.add(path) }
                    }
                },
                receiveInitial = false
            )

            db.mutatePublishBlocking { tx ->
                tx.putAll(
                    mapOf(
                        "/detail" to "1",
                        "/list/1" to "/detail"
                    )
                )
            }

            db.mutatePublishBlocking { tx ->
                tx.put("/list/2", "/fake")
                tx.put("/fake", "0")
                tx.put("/detail", "2")
                tx.put("/list/2", "/detail")
                tx.delete("/list/1")
                tx.put("/detail", "3")
            }

            assertEquals(
                listOf(
                    PathAndValue("/list/1", PathAndValue("/detail", "1")),
                    PathAndValue("/list/2", PathAndValue("/detail", "3")),
                ),
                updates
            )

            assertEquals(listOf("/list/1"), deletions)
        }
    }

    @Test
    fun testSchema() {
        buildDB().use { parent ->
            val parentUpdates = HashMap<String, Any>()
            parent.subscribe(object : Subscriber<Any>("subscriber", "%") {
                override fun onChanges(changes: ChangeSet<Any>) {
                    parentUpdates.putAll(changes.updates)
                }
            })

            parent.withSchema("child").use { child ->
                val childUpdates = HashMap<String, Any>()
                child.subscribe(object : Subscriber<Any>("subscriber", "%") {
                    override fun onChanges(changes: ChangeSet<Any>) {
                        childUpdates.putAll(changes.updates)
                    }
                })

                parent.mutatePublishBlocking { parentTx ->
                    parentTx.put("path", "p")
                    child.mutatePublishBlocking { childTx ->
                        childTx.put("path", "c")
                    }
                }

                assertEquals("parent should have correct value", "p", parent.get<String>("path"))
                assertEquals("child should have correct value", "c", child.get<String>("path"))

                assertEquals(
                    "parent subscriber should have gotten relevant updates",
                    mapOf("path" to "p"),
                    parentUpdates
                )
                assertEquals(
                    "child subscriber should have gotten relevant updates",
                    mapOf("path" to "c"),
                    childUpdates
                )
            }

            assertEquals(
                "parent should still be open and queryable after closing child",
                "p",
                parent.get<String>("path")
            )
        }
    }

    @Test
    fun testDurability() {
        buildDB().use { db ->
            db.mutatePublishBlocking { tx ->
                tx.putAll(mapOf("path1" to "value1", "path2" to "value2"))
            }

            try {
                db.mutatePublishBlocking { tx ->
                    tx.put("path3", "value3")
                    throw IllegalArgumentException("I failed")
                }
            } catch (_: IllegalArgumentException) {
                // ignore exception
            }
        }

        // open the db again
        val db2 = buildDB()
        assertEquals("value1", db2.get<String>("path1"))
        assertEquals("value2", db2.get<String>("path2"))
        assertTrue(db2.contains("path1"))
        assertNull("path3 should have been rolled back", db2.get("path3"))
        db2.close()
    }

    @Test
    fun testSharedPreferences() {
        val fallback =
            InstrumentationRegistry.getInstrumentation().targetContext.getSharedPreferences(
                "testPreferences",
                Context.MODE_PRIVATE
            )
        fallback.edit().clear().commit()
        fallback.edit().putBoolean("fboolean", true).putFloat("ffloat", 1.1f)
            .putInt("fint", 2).putLong("flong", 3).putString("fstring", "fallbackstring")
            .putBoolean("boolean", true).putFloat("float", 1.1f).putInt("int", 2)
            .putLong("long", 3).putString("string", "fallbackstring")
            .putInt("intboolean", 1).putLong("longboolean", 1).putString("stringboolean", "true").commit()
        buildDB().use { db ->
            // First set up the preferences without a fallback
            val prefsDb = db.withSchema("myprefs")
            val initPrefs = prefsDb.asSharedPreferences()
            initPrefs.edit().putBoolean("boolean", true).putFloat("float", 11.11f)
                .putInt("int", 22)
                .putLong("long", 33).putString("string", "realstring").commit()
            // Now set it up with the fallback (this ensures that we don't overwrite stuff in the database from the fallback)
            val prefs = prefsDb.asSharedPreferences(fallback)

            assertEquals(
                mapOf(
                    "fboolean" to true,
                    "ffloat" to 1.1f,
                    "fint" to 2,
                    "flong" to 3L,
                    "fstring" to "fallbackstring",
                    "boolean" to true,
                    "float" to 11.11f,
                    "int" to 22,
                    "long" to 33L,
                    "string" to "realstring",
                    "intboolean" to 1,
                    "longboolean" to 1L,
                    "stringboolean" to "true"
                ),
                prefs.all
            )

            db.withSchema("myprefs").use { prefsDB ->
                assertTrue(prefsDB.get("boolean") ?: false)
                assertTrue(prefs.getBoolean("boolean", false))
                assertTrue(prefs.getBoolean("fboolean", false))
                assertTrue(prefs.getBoolean("uboolean", true))
                assertTrue(prefs.getBoolean("intboolean", true))
                assertTrue(prefs.getBoolean("longboolean", true))
                assertTrue(prefs.getBoolean("stringboolean", true))

                assertEquals(11.11f, prefsDB.get<Float>("float"))
                assertEquals(11.11f, prefs.getFloat("float", 111.111f))
                assertEquals(1.1f, prefs.getFloat("ffloat", 111.111f))
                assertEquals(111.111f, prefs.getFloat("ufloat", 111.111f))

                assertEquals(22, prefsDB.get<Int>("int"))
                assertEquals(22, prefs.getInt("int", 222))
                assertEquals(2, prefs.getInt("fint", 222))
                assertEquals(222, prefs.getInt("uint", 222))

                assertEquals(33L, prefsDB.get<Long>("long"))
                assertEquals(33L, prefs.getLong("long", 333))
                assertEquals(3L, prefs.getLong("flong", 333))
                assertEquals(333L, prefs.getLong("ulong", 333))

                assertEquals("realstring", prefsDB.get<String>("string"))
                assertEquals("realstring", prefs.getString("string", "unknownstring"))
                assertEquals("fallbackstring", prefs.getString("fstring", "unknownstring"))
                assertEquals("unknownstring", prefs.getString("ustring", "unknownstring"))
            }
        }
    }

    @Test
    fun testPreferencesListener() {
        buildDB().use { db ->
            val prefs = db.withSchema("prefstest").asSharedPreferences()

            val updatedKeys = HashSet<String>()
            val listener =
                SharedPreferences.OnSharedPreferenceChangeListener { _, key ->
                    updatedKeys.add(
                        key!!
                    )
                }
            prefs.registerOnSharedPreferenceChangeListener(listener)

            prefs.edit().putString("string", "My String").putInt("int", 5).commit()
            assertEquals(mapOf("string" to "My String", "int" to 5), prefs.all)
            Thread.sleep(500)
            assertEquals(setOf("string", "int"), updatedKeys)

            updatedKeys.clear()
            prefs.edit().clear().commit()
            assertEquals(0, prefs.all.size)
            Thread.sleep(500)
            assertEquals(setOf("string", "int"), updatedKeys)

            updatedKeys.clear()
            prefs.unregisterOnSharedPreferenceChangeListener(listener)
            prefs.edit().putString("newstring", "My New String").commit()
            // wait a little bit for updates
            Thread.sleep(500)
            assertEquals(0, updatedKeys.size)
        }
    }

    @Test
    fun testTransactions() {
        buildDB().use { db ->
            val updatedPaths = HashSet<String>()

            db.subscribe(
                object : RawSubscriber<Any>("1", "") {
                    override fun onChanges(changes: RawChangeSet<Any>) {
                        changes.updates.forEach { (path, value) ->
                            updatedPaths.add(path)
                        }
                    }
                },
                false
            )

            val result = db.mutatePublishBlocking { tx ->
                tx.put("a", "a") // this should persist to the db
                val c = db.mutatePublishBlocking { nestedTx ->
                    try {
                        // None of the below should persist to the db
                        db.mutatePublishBlocking { subNestedTx ->
                            subNestedTx.put("b", "b")
                            db.mutatePublishBlocking { subSubNestedTx ->
                                subSubNestedTx.put("f", "f")
                            }
                            throw IllegalArgumentException("I failed!")
                        }
                    } catch (t: IllegalArgumentException) {
                        // ignore
                    }
                    nestedTx.put("c", "c")
                    "c"
                }

                tx.put("d", c) // this should persist to the db
                "e"
            }

            assertEquals("e", result)
            assertEquals(arrayListOf("a", "c", "d"), db.listPaths("%"))
            assertEquals(hashSetOf("a", "c", "d"), updatedPaths)
            assertEquals("c", db.get("d"))
        }
    }

    private fun buildDB(): DB {
        val db = DB.createOrOpen(
            InstrumentationRegistry.getInstrumentation().targetContext,
            filePath = File(tempDir, "testdb").toString(),
            password = "testpassword",
        )
        db.registerType(20, Message::class.java)
        return db
    }

    @Before
    fun setupTempDir() {
        tempDir = File(
            InstrumentationRegistry.getInstrumentation().targetContext.cacheDir,
            Random().nextLong().toString()
        )
        tempDir!!.mkdirs()
    }

    @After
    fun deleteTempDir() {
        tempDir?.deleteRecursively()
    }
}

internal data class Message(val body: String = "")
