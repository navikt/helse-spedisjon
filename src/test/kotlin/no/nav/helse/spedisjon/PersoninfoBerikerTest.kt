package no.nav.helse.spedisjon

import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class PersoninfoBerikerTest : AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    fun `Beriker fremtidig søknad med fødselsdato som et eget event`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        val duplikatkontroll = hentDuplikatkontroll(FØDSELSNUMMER)
        testRapid.sendTestMessage(personinfoV3Løsning(duplikatkontroll))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))

        assertEquals(3, testRapid.inspektør.size)
        val beriketSøknad = testRapid.inspektør.message(2)
        assertEquals("NY", beriketSøknad["status"].textValue())
        assertEquals("ny_søknad_beriket", beriketSøknad["@event_name"].textValue())
        assertEquals("1950-10-27", beriketSøknad["fødselsdato"].asText())
    }

    @Test
    fun `Beriker ikke inntektsmelding`() {
        testRapid.sendTestMessage(inntektmelding(FØDSELSNUMMER))
        val duplikatkontroll = hentDuplikatkontroll(FØDSELSNUMMER)
        testRapid.sendTestMessage(personinfoV3Løsning(duplikatkontroll))
        assertEquals(1, testRapid.inspektør.size)
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

    private fun inntektmelding(fnr: String = FØDSELSNUMMER) =
        """
        {
            "inntektsmeldingId": "id",
            "arbeidstakerFnr": "$fnr",
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

    private fun søknad(status: String = "FREMTIDIG", type: String = "ARBEIDSTAKERE") = """
        {
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

    """.trimIndent()

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient)
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
    }
}
