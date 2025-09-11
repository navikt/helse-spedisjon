package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import java.time.LocalDateTime
import java.util.*
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class SendteSelvstendigSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser ikke inn selvstendige søknader der ventetid er null`() {
        testRapid.sendTestMessage(søknad(arbeidssituasjon = "SELVSTENDIG_NARINGSDRIVENDE", ventetid = null))
        Assertions.assertEquals(0, antallMeldinger())
    }

    @Test
    fun `leser sendte selvstendige søknader`() {
        testRapid.sendTestMessage(søknad(arbeidssituasjon = "SELVSTENDIG_NARINGSDRIVENDE"))
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("sendt_søknad_selvstendig")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `leser ikke sendte selvstendige søknader med annen arbeidssituasjon`() {
        testRapid.sendTestMessage(søknad(arbeidssituasjon = "JORDBRUKER"))
        Assertions.assertEquals(0, antallMeldinger(FØDSELSNUMMER))
    }

    @Test
    fun `leser ikke sendte selvstendige søknader med fødselsdag utenfor 30 til 31`() {
        (1..29).forEach {
            val fnr = FØDSELSNUMMER.replaceRange(0, 2, it.toString().padStart(2, '0'))
            testRapid.sendTestMessage(søknad(arbeidssituasjon = "SELVSTENDIG_NARINGSDRIVENDE", fnr = fnr))
            Assertions.assertEquals(0, antallMeldinger(fnr), "Forventet 0 meldinger for fødselsnummer $fnr")
        }
    }

    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        SendteSelvstendigSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    private companion object {
        @Language("JSON")
        private fun søknad(arbeidssituasjon: String, fnr: String = FØDSELSNUMMER, ventetid: String? = """{"fom" : "2020-01-01","tom" : "2020-01-16"}""") = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$fnr",
            "arbeidsgiver": null,
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "$OPPRETTET_DATO",
            "soknadsperioder": [],
            "fravar": null,
            "status": "SENDT",
            "type": "SELVSTENDIGE_OG_FRILANSERE",
            "arbeidssituasjon": "$arbeidssituasjon",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01",
            "selvstendigNaringsdrivende": {
                "ventetid": $ventetid
            }
        }
        """
    }
}
