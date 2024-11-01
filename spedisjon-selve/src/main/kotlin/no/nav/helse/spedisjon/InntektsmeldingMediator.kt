package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.navikt.tbd_libs.speed.PersonResponse.Adressebeskyttelse
import com.github.navikt.tbd_libs.speed.SpeedClient
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.withMDC
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class InntektsmeldingMediator (
    dataSource: DataSource,
    private val speedClient: SpeedClient,
    private val inntektsmeldingDao: InntektsmeldingDao = InntektsmeldingDao(dataSource),
    private val meldingMediator: MeldingMediator,
    private val inntektsmeldingTimeoutSekunder: Long = 1
) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

    fun lagreInntektsmelding(inntektsmelding: Melding.Inntektsmelding, messageContext: MessageContext) {
        val ønsketPublisert = LocalDateTime.now().plusSeconds(inntektsmeldingTimeoutSekunder)
        meldingMediator.onMelding(inntektsmelding, messageContext)
        if (!inntektsmeldingDao.leggInn(inntektsmelding, ønsketPublisert)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen

        if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
            logg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            sikkerlogg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            ekspeder(messageContext)
        }
    }

    fun ekspeder(messageContext: MessageContext) {
        inntektsmeldingDao.hentSendeklareMeldinger(inntektsmeldingTimeoutSekunder) { inntektsmelding, antallInntektsmeldingerMottatt ->
            val callId = UUID.randomUUID().toString()
            withMDC("callId" to callId) {
                val (personinfo, historiskeIdenter, identer) = Personinformasjon.innhent(speedClient, inntektsmelding.melding, callId)

                val støttes = personinfo.adressebeskyttelse !in setOf(Adressebeskyttelse.STRENGT_FORTROLIG, Adressebeskyttelse.STRENGT_FORTROLIG_UTLAND)
                when (støttes) {
                    true -> {
                        val berikelse = Berikelse(
                            fødselsdato = personinfo.fødselsdato,
                            dødsdato = personinfo.dødsdato,
                            aktørId = identer.aktørId,
                            historiskeFolkeregisteridenter = historiskeIdenter.fødselsnumre
                        )
                        ekspederInntektsmelding(messageContext, berikelse, inntektsmelding, antallInntektsmeldingerMottatt)
                    }
                    false -> sikkerlogg.info("Personen støttes ikke ${identer.aktørId}")
                }
            }
        }
    }

    private fun ekspederInntektsmelding(messageContext: MessageContext, berikelse: Berikelse, inntektsmelding: SendeklarInntektsmelding, antallInntektsmeldingMottatt: Int) {
        val beriketMelding = berikelse.berik(inntektsmelding.melding)
        sikkerlogg.info("Ekspederer inntektsmelding med fødselsnummer: ${inntektsmelding.fnr} og orgnummer: ${inntektsmelding.orgnummer}")
        val beriketMeldingMedFlagg = flaggFlereInntektsmeldinger(beriketMelding, antallInntektsmeldingMottatt)
        messageContext.publish(inntektsmelding.fnr, beriketMeldingMedFlagg.toString())
    }

    private fun flaggFlereInntektsmeldinger(beriketMelding: ObjectNode, antallInntektsmeldinger: Int): JsonNode =
        beriketMelding.put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)

    fun onRiverError(error: String) {
        meldingMediator.onRiverError(error)
    }
}
