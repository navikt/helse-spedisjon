package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PersonStøttesIkkeTest : AbstractRiverTest() {

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }

    @Test
    fun `Ber om attributt støttes i berikelse av person`() {
        testRapid.sendTestMessage(søknad(status = "FREMTIDIG"))
        val personInfoBerikelseBehov = testRapid.inspektør.message(0)
        assertTrue(personInfoBerikelseBehov.get("HentPersoninfoV3").get("attributter").map { it.asText() }.toList().contains("støttes"))
    }

    @Test
    fun `fremtidig_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "FREMTIDIG"))
        sendBerikelse(støttes = false)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov")
    }

    @Test
    fun `ny_søknad til person som ikke støttes lagres, men sendes ikke videre`() {
        testRapid.sendTestMessage(søknad(status = "NY"))
        sendBerikelse(støttes = false)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov")
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
        sendBerikelse(støttes = false)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov")
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
        sendBerikelse(støttes = false)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov")
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
        sendBerikelse(støttes = false)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        val inntektsmeldingMediator = InntektsmeldingMediator(dataSource)
        val personBerikerMediator = PersonBerikerMediator(MeldingDao(dataSource), BerikelseDao(dataSource), meldingMediator)
        LogWrapper(testRapid, meldingMediator = meldingMediator).apply {
            Inntektsmeldinger(rapidsConnection = this, inntektsmeldingMediator = inntektsmeldingMediator)
            NyeSøknader(this, meldingMediator)
            FremtidigSøknaderRiver(this, meldingMediator)
            SendteSøknaderArbeidsgiver(this, meldingMediator)
            SendteSøknaderNav(this, meldingMediator)
        }
        PersoninfoBeriker(testRapid, personBerikerMediator)
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