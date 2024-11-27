package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearAllMocks
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import java.util.UUID

internal abstract class AbstractRiverTest : AbstractDatabaseTest() {
    protected val testRapid = TestRapid()

    protected abstract fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste)

    protected companion object {
        private val objectMapper = jacksonObjectMapper()
    }

    @AfterEach
    internal fun `clear messages`() {
        testRapid.reset()
        clearAllMocks()
    }

    @BeforeEach
    fun `create river`() {
        createRiver(testRapid, meldingstjeneste)
    }

    protected fun assertSendteEvents(vararg events: String) {
        val sendteEvents = when (testRapid.inspektør.size == 0) {
            true -> emptyList<String>()
            false -> (0 until testRapid.inspektør.size).map { testRapid.inspektør.message(it).path("@event_name").asText() }
        }
        assertEquals(events.toList(), sendteEvents)
    }

    protected fun String.json(block: (node: ObjectNode) -> Unit) : String {
        val node = objectMapper.readTree(this) as ObjectNode
        block(node)
        return node.toString()
    }
}

class LokalMeldingtjeneste : Meldingtjeneste {
    private val meldingsliste = mutableListOf<MeldingDto>()
    val meldinger get() = meldingsliste.toList()

    override fun nyMelding(meldingsdetaljer: NyMeldingRequest): NyMeldingResponse {
        val melding = meldingsliste.firstOrNull { it.duplikatkontroll == meldingsdetaljer.duplikatkontroll } ?:
            MeldingDto(
                type = meldingsdetaljer.type,
                fnr = meldingsdetaljer.fnr,
                internDokumentId = UUID.randomUUID(),
                eksternDokumentId = meldingsdetaljer.eksternDokumentId,
                rapportertDato = meldingsdetaljer.rapportertDato,
                duplikatkontroll = meldingsdetaljer.duplikatkontroll,
                jsonBody = meldingsdetaljer.jsonBody
            ).also { meldingsliste.add(it) }
        return NyMeldingResponse(melding.internDokumentId)
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        return HentMeldingerResponse(
            meldinger = meldingsliste.filter { dto ->
                dto.internDokumentId in interneDokumentIder
            }
        )
    }
}