package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import java.util.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class NyeFrilansSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser nye søknader`() {
        testRapid.sendTestMessage("""
{
  "id": "${UUID.randomUUID()}",
  "type": "SELVSTENDIGE_OG_FRILANSERE",
  "status": "NY",
  "fnr": "$FØDSELSNUMMER",
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
  "opprettet": "2023-07-31T02:00:00",
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
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("ny_søknad_frilans")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        NyeFrilansSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }
}
