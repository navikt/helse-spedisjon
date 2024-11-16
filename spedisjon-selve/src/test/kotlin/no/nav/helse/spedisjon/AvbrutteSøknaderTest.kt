package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.mockk
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class AvbrutteSøknaderTest : AbstractRiverTest() {
    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient, mockk(relaxed = true))
        AvbrutteSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        AvbrutteArbeidsledigSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    @Test
    fun `Leser, beriker, videresender avbrutte søknader`() {
        testRapid.sendTestMessage(AVBRUTT_SØKNAD)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_søknad")
    }
    @Test
    fun `Leser, beriker, videresender avbrutte arbeidsledig-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_ARBEIDSLEDIG_SØKNAD)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_arbeidsledig_søknad")
    }

    private companion object {
        @Language("JSON")
        private val AVBRUTT_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "type": "ARBEIDSTAKERE",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_ARBEIDSLEDIG_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "type": "ARBEIDSLEDIG",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }


}

