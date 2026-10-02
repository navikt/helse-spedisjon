package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearAllMocks
import java.util.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.kotlin.jacksonObjectMapper

internal abstract class AbstractRiverTest : AbstractDatabaseTest() {
    protected val testRapid = TestRapid()

    protected abstract fun createRiver(
        rapidsConnection: RapidsConnection,
        meldingtjeneste: Meldingtjeneste
    )

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
        val sendteEvents =
            when (testRapid.inspektør.size == 0) {
                true -> emptyList<String>()
                false ->
                    (0 until testRapid.inspektør.size).map {
                        testRapid.inspektør
                            .message(it)
                            .path("@event_name")
                            .asString()
                    }
            }
        assertEquals(events.toList(), sendteEvents)
    }

    protected fun String.json(block: (node: ObjectNode) -> Unit): String {
        val node = objectMapper.readTree(this) as ObjectNode
        block(node)
        return node.toString()
    }
}

class TestMeldingtjeneste : Meldingtjeneste {
    private val meldingsliste = mutableListOf<MeldingDto>()
    val meldinger get() = meldingsliste.toList()

    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val melding =
            meldingsliste.firstOrNull { it.duplikatkontroll == request.duplikatkontroll }
                ?: MeldingDto(
                    type = request.type,
                    fnr = request.fnr,
                    internDokumentId = UUID.randomUUID(),
                    eksternDokumentId = request.eksternDokumentId,
                    duplikatkontroll = request.duplikatkontroll,
                    jsonBody = request.jsonBody
                ).also { meldingsliste.add(it) }
        return NyMeldingResponse(melding.internDokumentId)
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse =
        HentMeldingerResponse(
            meldinger =
                meldingsliste.filter { dto ->
                    dto.internDokumentId in interneDokumentIder
                }
        )
}
