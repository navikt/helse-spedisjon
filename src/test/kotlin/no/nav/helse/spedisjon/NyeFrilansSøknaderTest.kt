package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class NyeFrilansSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser nye søknader`() {
        testRapid.sendTestMessage("""
{
  "id": "id",
  "type": "SELVSTENDIGE_OG_FRILANSERE",
  "status": "NY",
  "fnr": "$FØDSELSNUMMER",
  "aktorId": "$AKTØR",
  "sykmeldingId": "227c0743-5ac4-4ca8-ad41-38145bf06bd6",
  "arbeidsgiver": null,
  "arbeidssituasjon": "FRILANSER",
  "korrigerer": null,
  "korrigertAv": null,
  "soktUtenlandsopphold": null,
  "arbeidsgiverForskutterer": null,
  "fom": "2023-08-01",
  "tom": "2023-08-23",
  "dodsdato": null,
  "startSyketilfelle": "2023-07-03",
  "arbeidGjenopptatt": null,
  "sykmeldingSkrevet": "2023-07-03T02:00:00",
  "opprettet": "2023-08-25T14:41:11.518772",
  "opprinneligSendt": null,
  "sendtNav": null,
  "sendtArbeidsgiver": null,
  "egenmeldinger": null,
  "fravarForSykmeldingen": null,
  "papirsykmeldinger": null,
  "fravar": null,
  "andreInntektskilder": [],
  "soknadsperioder": [
    {
      "fom": "2023-08-01",
      "tom": "2023-08-23",
      "sykmeldingsgrad": 100,
      "faktiskGrad": null,
      "avtaltTimer": null,
      "faktiskTimer": null,
      "sykmeldingstype": "AKTIVITET_IKKE_MULIG",
      "grad": 100
    }
  ],
  "avsendertype": "BRUKER",
  "ettersending": false,
  "mottaker": null,
  "egenmeldtSykmelding": false,
  "yrkesskade": null,
  "arbeidUtenforNorge": false,
  "harRedusertVenteperiode": false,
  "behandlingsdager": null,
  "permitteringer": [],
  "merknaderFraSykmelding": null,
  "egenmeldingsdagerFraSykmelding": null,
  "merknader": null,
  "sendTilGosys": null,
  "utenlandskSykmelding": false
}""")
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "ny_frilans_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        val personBerikerMediator = PersonBerikerMediator(MeldingDao(dataSource), BerikelseDao(dataSource), meldingMediator)
        NyeFrilansSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        PersoninfoBeriker(
            rapidsConnection = rapidsConnection,
            personBerikerMediator = personBerikerMediator
        )
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }
}
