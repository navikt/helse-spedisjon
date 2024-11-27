package no.nav.helse.spedisjon.async

import ch.qos.logback.classic.Logger
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
import java.util.*

internal class LogWrapperTest {
    private val rapid = TestRapid()

    private val appender = ListAppender<ILoggingEvent>().apply { start() }

    private val log = (LoggerFactory.getLogger("tjenestekall") as Logger).apply {
        addAppender(appender)
    }

    private val meldingtjeneste = mockk<Meldingtjeneste>()
    private val speedClient = mockk<SpeedClient> {
        every { hentPersoninfo(any(), any()) } returns Result.Ok(mockk(relaxed = true))
        every { hentHistoriskeFødselsnumre(any(), any()) } returns Result.Ok(mockk(relaxed = true))
        every { hentFødselsnummerOgAktørId(any(), any()) } returns Result.Ok(mockk(relaxed = true))
    }
    private val ekspederingMediator = EkspederingMediator(
        dao = mockk { every { meldingEkspedert(any()) } returns true },
        rapidsConnection = TestRapid()
    )
    private val mediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)

    @BeforeEach
    fun setup() {
        appender.list.clear()
        rapid.reset()
        every { meldingtjeneste.nyMelding(any()) }.answers {
            NyMeldingResponse.OK(UUID.randomUUID())
        }
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
            mediator.onMelding(Melding.NySøknad(
                internId = UUID.randomUUID(),
                meldingsdetaljer = Meldingsdetaljer(
                    type = "ny_søknad",
                    fnr = "fnr",
                    eksternDokumentId = UUID.randomUUID(),
                    rapportertDato = LocalDateTime.now(),
                    duplikatnøkkel = listOf("en_nøkkel"),
                    jsonBody = packet.toJson()
                )
            )
            )
        }
        override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
            mediator.onRiverError("Ukjent melding:\n$problems")
        }
    }

}
