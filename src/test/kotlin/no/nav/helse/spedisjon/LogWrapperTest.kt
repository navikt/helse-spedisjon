package no.nav.helse.spedisjon

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

internal class LogWrapperTest {
    private val rapid = TestRapid()

    private val appender = ListAppender<ILoggingEvent>().apply { start() }

    private val log = (LoggerFactory.getLogger(this::class.java) as ch.qos.logback.classic.Logger).apply {
        addAppender(appender)
    }

    @BeforeEach
    fun setup() {
        appender.list.clear()
        rapid.reset()
    }

    @Test
    fun `logger ingenting når melding er gjenkjent`() {
        LogWrapper(rapid, log).apply {
            TestRiver(this, problemsCollector)
            TestRiver(this, problemsCollector) { validate { it.requireKey("a_key_not_set") } }
        }
        rapid.sendTestMessage("{}")
        assertTrue(appender.list.isEmpty())
    }

    @Test
    fun `logger når melding ikke er gjenkjent`() {
        LogWrapper(rapid, log).apply {
            TestRiver(this, problemsCollector) { validate { it.requireKey("a_key_not_set_1") } }
            TestRiver(this, problemsCollector) { validate { it.requireKey("a_key_not_set_2") } }
        }
        rapid.sendTestMessage("{}")
        assertTrue(appender.list.filter { it.formattedMessage.contains("Kunne ikke forstå melding") }.size == 1)
        assertTrue(appender.list.filter { it.formattedMessage.contains("a_key_not_set_1") }.size == 1)
        assertTrue(appender.list.filter { it.formattedMessage.contains("a_key_not_set_2") }.size == 1)
    }

    private class TestRiver(
        rapidsConnection: RapidsConnection,
        private val problemsCollector: ProblemsCollector,
        validation: River.() -> Unit = {}
    ) :
        River.PacketListener {
        init {
            River(rapidsConnection).apply(validation).register(this)
        }

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {}
        override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
            problemsCollector.add("Test river", problems)
        }
    }

}
