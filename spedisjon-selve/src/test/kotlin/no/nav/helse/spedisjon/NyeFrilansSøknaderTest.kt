package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
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
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad_frilans")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient)
        NyeFrilansSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }
}
