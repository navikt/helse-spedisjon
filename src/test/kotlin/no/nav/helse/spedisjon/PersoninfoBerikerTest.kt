package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PersoninfoBerikerTest : AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    fun `Beriker fremtidig søknad`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        assertBeriket("ny_søknad") {
            assertEquals("NY", it["status"].textValue())
            assertTrue(it["fremtidig_søknad"].asBoolean())
        }
    }

    @Test
    fun `Beriker inntektsmelding`() {
        testRapid.sendTestMessage(inntektmelding())
        assertBeriket("inntektsmelding")
    }

    @Test
    fun `Beriker ny søknad`() {
        testRapid.sendTestMessage(søknad("NY"))
        assertBeriket("ny_søknad") {
            assertEquals("NY", it["status"].textValue())
            assertNull(it["fremtidig_søknad"])
        }
    }

    @Test
    fun `Beriker sendt søknad arbeidsgiver`() {
        testRapid.sendTestMessage(sendtSøknadArbeidsgiver)
        assertBeriket("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `Beriker sendt søknad nav`() {
        testRapid.sendTestMessage(sendtSøknadNav)
        assertBeriket("sendt_søknad_nav")
    }

    @Test
    fun `Beriker ikke en melding som allerede er beriket`() {
        testRapid.sendTestMessage(sendtSøknadNav)
        assertBeriket("sendt_søknad_nav")
        testRapid.sendTestMessage(personinfoV3Løsning(hentDuplikatkontroll(FØDSELSNUMMER)))
        assertEquals(3, testRapid.inspektør.size)
    }

    private fun assertBeriket(forventetEvent: String, assertions: (jsonNode: JsonNode) -> Unit = {}) {
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        val duplikatkontroll = hentDuplikatkontroll(FØDSELSNUMMER)
        testRapid.sendTestMessage(personinfoV3Løsning(duplikatkontroll))
        assertEquals(3, testRapid.inspektør.size)
        assertEquals(forventetEvent, testRapid.inspektør.message(0).path("@event_name").asText())
        assertEquals("behov", testRapid.inspektør.message(1).path("@event_name").asText())
        val beriket = testRapid.inspektør.message(2)
        assertEquals("${forventetEvent}_beriket", beriket["@event_name"].textValue())
        assertEquals("1950-10-27", beriket.path("fødselsdato").asText())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(beriket.path("@id").asText(), testRapid.inspektør.message(0).path("@id").asText())
        assertions(beriket)
    }

    private fun personinfoV3Løsning(duplikatkontroll: String?) =
        """
        {
            "@id": "514ae64c-a692-4d83-9a9a-7308a5453986",
            "@behovId": "9a06d800-f6dd-423f-99bc-6dde4f017931",
            "@behov": ["HentPersoninfoV3"],
            "@final": true,
            "HentPersoninfoV3": {
                "ident": "27105027856",
                "attributter": ["fødselsdato", "aktørId"]
            },
            "@opprettet": "2022-06-27T15:01:43.756488972",
            "spedisjonMeldingId": "$duplikatkontroll",
            "@løsning": {
                "HentPersoninfoV3": {
                    "aktørId": "$AKTØR",
                    "fødselsdato": "1950-10-27"
                }
            }
        }
        """

    private fun inntektmelding() =
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
            "foersteFravaersdag": "2020-01-01"
        }
        """

    private fun søknad(status: String) = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "$OPPRETTET_DATO",
            "type": "ARBEIDSTAKERE",
            "soknadsperioder": [],
            "status": "$status",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }

    """

    private val sendtSøknadArbeidsgiver = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "sendtArbeidsgiver": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "egenmeldinger": [],
            "fravar": [],
            "status": "SENDT",
            "type": "ARBEIDSTAKERE",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

    private val sendtSøknadNav = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "egenmeldinger": [],
            "fravar": [],
            "status": "SENDT",
            "type": "ARBEIDSTAKERE",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource), aktørregisteretClient)
        PersoninfoBeriker(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
        FremtidigSøknaderRiver(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
        Inntektsmeldinger(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
        NyeSøknader(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
        SendteSøknaderArbeidsgiver(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
        SendteSøknaderNav(
            rapidsConnection = testRapid,
            meldingMediator = meldingMediator
        )
    }
}
