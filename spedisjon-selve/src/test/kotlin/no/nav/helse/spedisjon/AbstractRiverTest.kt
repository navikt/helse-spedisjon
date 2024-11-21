package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearAllMocks
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach

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
        createRiver(testRapid, LokalMeldingtjeneste(MeldingDao(dataSource)))
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