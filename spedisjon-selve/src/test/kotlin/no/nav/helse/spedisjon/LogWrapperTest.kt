package no.nav.helse.spedisjon

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.Result
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.UUID

internal class LogWrapperTest {
    private val rapid = TestRapid()

    private val appender = ListAppender<ILoggingEvent>().apply { start() }

    private val log = (LoggerFactory.getLogger("tjenestekall") as ch.qos.logback.classic.Logger).apply {
        addAppender(appender)
    }

    private val meldingMock:MeldingDao = mockk()
    private val speedClient = mockk<SpeedClient> {
        every { hentPersoninfo(any(), any()) } returns Result.Ok(mockk(relaxed = true))
        every { hentHistoriskeFødselsnumre(any(), any()) } returns Result.Ok(mockk(relaxed = true))
        every { hentFødselsnummerOgAktørId(any(), any()) } returns Result.Ok(mockk(relaxed = true))
    }
    private val mediator = MeldingMediator(meldingMock, speedClient, mockk(relaxed = true))

    @BeforeEach
    fun setup() {
        appender.list.clear()
        rapid.reset()
        every { meldingMock.leggInn(any()) }.answers { true }
    }

    @Test
    fun `logger ingenting når melding er gjenkjent`() {
        LogWrapper(rapid, mediator).apply {
            TestRiver(this, mediator) { validate { it.requireKey("a_key", "b_key") } }
            TestRiver(this, mediator) { validate { it.requireKey("a_key_not_set", "b_key_not_set") } }
        }
        rapid.sendTestMessage("{\"a_key\": \"foo\", \"b_key\": \"bar\"}")
        assertTrue(appender.list.filter { it.formattedMessage.contains("kunne ikke gjenkjenne melding") }.isEmpty())
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

        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
            mediator.onMelding(TestMelding(packet), context)
        }
        override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
            mediator.onRiverError("Ukjent melding:\n$problems")
        }
    }

}

internal class TestMelding(packet: JsonMessage) : Melding(packet, meldingsdetaljer = Meldingsdetaljer(type = "test_melding", eksternDokumentId = UUID.randomUUID(), rapportertDato = LocalDateTime.now())) {
    override fun fødselsnummer(): String = "123412341234"
    override fun duplikatnøkkel(): String = "1"
}
