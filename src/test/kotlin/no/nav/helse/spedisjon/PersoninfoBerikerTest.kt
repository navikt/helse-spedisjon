package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class PersoninfoBerikerTest {
    protected val testRapid = TestRapid()
    private val personinfoBeriker = PersoninfoBeriker(
        rapidsConnection = testRapid
    )


    @Test
    fun `leser personinfoberikelse`() {
        testRapid.sendTestMessage("""
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
    "spedisjonMeldingId": "sha512",
	"@løsning": {
		"HentPersoninfoV3": {
			"aktørId": "2844331383393",
			"fødselsdato": "1950-10-27"
		}
	}
}""")
        assertTrue(personinfoBeriker.lestMelding)
    }
}
