package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PersonStøttesIkkeTest : AbstractRiverTest() {

    @Test
    fun `fremtidig_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "FREMTIDIG"))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `ny_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "NY"))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `søknad_nav til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(
            søknad(
                status = "SENDT", ekstralinjer = listOf(
                    """"sendtNav": "${LocalDateTime.now()}"""",
                    """"fravar": []"""
                )
            )
        )
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `søknad_arbeidsgiver til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(
            søknad(
                status = "SENDT", ekstralinjer = listOf(
                    """"sendtArbeidsgiver": "${LocalDateTime.now()}"""",
                    """"fravar": []"""
                )
            )
        )
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `inntektsmelding til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(
            """
        {
            "inntektsmeldingId": "id",
            "arbeidstakerFnr": "$FØDSELSNUMMER",
            "arbeidstakerAktorId": "$AKTØR",
            "virksomhetsnummer": "1234",
            "arbeidsgivertype": "BEDRIFT",
            "beregnetInntekt": "1000",
            "mottattDato": "${LocalDateTime.now()}",
            "endringIRefusjoner": [],
            "arbeidsgiverperioder": [],
            "ferieperioder": [],
            "status": "GYLDIG",
            "arkivreferanse": "arkivref",
            "foersteFravaersdag": "2020-01-01",
            "matcherSpleis": true
        }
        """
        )
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed(støttes = false)
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient)
        val inntektsmeldingMediator = InntektsmeldingMediator(dataSource, speedClient, meldingMediator = meldingMediator)
        LogWrapper(testRapid, meldingMediator = meldingMediator).apply {
            Inntektsmeldinger(rapidsConnection = this, inntektsmeldingMediator = inntektsmeldingMediator)
            NyeSøknader(this, meldingMediator)
            FremtidigSøknaderRiver(this, meldingMediator)
            SendteSøknaderArbeidsgiver(this, meldingMediator)
            SendteSøknaderNav(this, meldingMediator)
        }
    }

    private fun søknad(
        status: String = "FREMTIDIG",
        type: String = "ARBEIDSTAKERE",
        ekstralinjer: List<String> = emptyList()
    ) = """
        {
            ${if (ekstralinjer.isNotEmpty()) ekstralinjer.joinToString(postfix = ",") else ""}
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "$OPPRETTET_DATO",
            "type": "$type",
            "soknadsperioder": [],
            "status": "$status",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }
    """
}