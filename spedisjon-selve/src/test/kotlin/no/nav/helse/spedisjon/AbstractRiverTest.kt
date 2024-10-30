package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.speed.HistoriskeIdenterResponse
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.PersonResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import java.time.LocalDate
import javax.sql.DataSource

internal abstract class AbstractRiverTest : AbstractDatabaseTest() {
    protected val testRapid = TestRapid()

    protected abstract fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource)

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
        createRiver(testRapid, dataSource)
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