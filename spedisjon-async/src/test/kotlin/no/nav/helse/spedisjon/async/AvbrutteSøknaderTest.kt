package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

internal class AvbrutteSøknaderTest : AbstractRiverTest() {
    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        AvbrutteSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    @Test
    fun `Leser, beriker, videresender avbrutte søknader`() {
        testRapid.sendTestMessage(AVBRUTT_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte arbeidsledig-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_ARBEIDSLEDIG_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_arbeidsledig_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte frilans-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_FRILANS_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_frilanser_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte selvstendig-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_SELVSTENDIG_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_selvstendig_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte fisker-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_FISKER_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_fisker_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte jordbruker-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_JORDBRUKER_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_jordbruker_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte barnepasser-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_BARNEPASSER_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_barnepasser_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `Leser, beriker, videresender avbrutte annet-søknader`() {
        testRapid.sendTestMessage(AVBRUTT_ANNET_SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("avbrutt_annet_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
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
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "ARBEIDSTAKER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_ARBEIDSLEDIG_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "ARBEIDSLEDIG",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_SELVSTENDIG_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "SELVSTENDIG_NARINGSDRIVENDE",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_FRILANS_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "FRILANSER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_BARNEPASSER_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "BARNEPASSER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_FISKER_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "FISKER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_JORDBRUKER_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "JORDBRUKER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

        private val AVBRUTT_ANNET_SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "opprettet": "$OPPRETTET_DATO",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "AVBRUTT",
            "arbeidssituasjon": "ANNET",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }


}

