package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

internal class PersonStøttesIkkeTest : AbstractRiverTest() {

    @Test
    fun `fremtidig_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "FREMTIDIG"))
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `ny_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "NY"))
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
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
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
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
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `inntektsmelding til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(
            """
        {
            "inntektsmeldingId": "${UUID.randomUUID()}",
            "arbeidstakerFnr": "$FØDSELSNUMMER",
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
            "format": "Inntektsmelding"
        }
        """
        )
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed(støttes = false)
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        val inntektsmeldingDao = InntektsmeldingDao(meldingtjeneste, ::dataSource)
        val inntektsmeldingMediator = InntektsmeldingMediator(inntektsmeldingDao, ekspederingMediator)
        LogWrapper(testRapid, meldingMediator).apply {
            LpsOgAltinnInntektsmeldinger(this, meldingMediator, inntektsmeldingMediator)
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
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "$OPPRETTET_DATO",
            "type": "$type",
            "soknadsperioder": [],
            "status": "$status",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }
    """
}
