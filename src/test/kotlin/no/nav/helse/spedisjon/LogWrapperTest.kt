package no.nav.helse.spedisjon

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.mockk.mockk
import no.nav.helse.rapids_rivers.*
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

internal class LogWrapperTest {
    private val rapid = TestRapid()

    private val appender = ListAppender<ILoggingEvent>().apply { start() }

    private val log = (LoggerFactory.getLogger("tjenestekall") as ch.qos.logback.classic.Logger).apply {
        addAppender(appender)
    }

    private val mediator = MeldingMediator(mockk(), mockk(), true)

    @BeforeEach
    fun setup() {
        appender.list.clear()
        rapid.reset()
    }

    @Test
    fun `logger ingenting når melding er gjenkjent`() {
        LogWrapper(rapid, mediator).apply {
            TestRiver(this, mediator) { validate { it.requireKey("a_key", "b_key") } }
            TestRiver(this, mediator) { validate { it.requireKey("a_key_not_set", "b_key_not_set") } }
        }
        rapid.sendTestMessage("{\"a_key\": \"foo\", \"b_key\": \"bar\"}")
        assertTrue(appender.list.isEmpty())
    }

    @Test
    fun `logger når melding ikke er gjenkjent`() {
        LogWrapper(rapid, mediator).apply {
            TestRiver(this, mediator) { validate { it.requireKey("a_key_not_set_1") } }
            TestRiver(this, mediator) { validate { it.requireKey("a_key_not_set_2") } }
        }
        rapid.sendTestMessage("{}")
        assertTrue(appender.list.filter { it.formattedMessage.contains("kunne ikke gjenkjenne melding") }.size == 1)
        assertTrue(appender.list.filter { it.formattedMessage.contains("a_key_not_set_1") }.size == 1)
        assertTrue(appender.list.filter { it.formattedMessage.contains("a_key_not_set_2") }.size == 1)
    }

    private class TestRiver(
        rapidsConnection: RapidsConnection,
        private val mediator: MeldingMediator,
        validation: River.() -> Unit = {}
    ) :
        River.PacketListener {
        init {
            River(rapidsConnection).apply(validation).register(this)
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            mediator.onPacket(packet, "b_key", "a_key")
        }
        override fun onError(problems: MessageProblems, context: MessageContext) {
            mediator.onRiverError("Ukjent melding:\n$problems")
        }
    }

}
