package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PersoninfoBerikerRetryTest : AbstractRiverTest() {

    @Test
    fun `skal publisere behov på nytt om vi ikke har fått løsning`() {
        val berikelseDao = BerikelseDao(dataSource)
        berikelseDao.behovEtterspurt("fnr", "duplikatkontroll", listOf("foo", "bar"), LocalDateTime.now().minusMinutes(16))
        assertEquals(0, testRapid.inspektør.size)
        testRapid.sendTestMessage("""{"@event_name": "ping"}""")
        assertEquals(1, testRapid.inspektør.size)
        assertEquals("behov", testRapid.inspektør.message(0).path("@event_name").asText())
        assertEquals(listOf("foo", "bar"), testRapid.inspektør.message(0).path("HentPersoninfoV3").path("attributter").toList().map { it.asText() })
        assertEquals("fnr", testRapid.inspektør.message(0).path("HentPersoninfoV3").path("ident").asText())
        assertEquals("duplikatkontroll".padEnd(128, ' '), testRapid.inspektør.message(0).path("spedisjonMeldingId").asText())
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        PersoninfoBerikerRetry(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        )
    }

}