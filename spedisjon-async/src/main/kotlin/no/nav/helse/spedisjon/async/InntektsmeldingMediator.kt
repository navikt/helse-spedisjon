package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDateTime
import org.slf4j.LoggerFactory

internal class InntektsmeldingMediator(
    private val inntektsmeldingDao: InntektsmeldingDao,
    private val ekspederingMediator: EkspederingMediator,
    private val inntektsmeldingTimeoutSekunder: Long = 1
) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val objectmapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    fun lagreInntektsmelding(
        inntektsmelding: Melding.Inntektsmelding,
        ønsketPublisert: LocalDateTime = LocalDateTime.now().plusSeconds(inntektsmeldingTimeoutSekunder),
        mottatt: LocalDateTime = LocalDateTime.now()
    ) {
        if (!inntektsmeldingDao.leggInn(inntektsmelding, ønsketPublisert, mottatt)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen

        if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
            logg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            sikkerlogg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            ekspeder()
        }
    }

    fun ekspeder() {
        inntektsmeldingDao.hentSendeklareMeldinger(inntektsmeldingTimeoutSekunder) { inntektsmelding, antallInntektsmeldingerMottatt ->
            val beriketMelding = beriketInntektsmelding(inntektsmelding, antallInntektsmeldingerMottatt)
            ekspederingMediator.videresendMelding(inntektsmelding.fnr, inntektsmelding.melding.internId, beriketMelding)
        }
    }

    private fun beriketInntektsmelding(inntektsmelding: SendeklarInntektsmelding, antallInntektsmeldingMottatt: Int): BeriketMelding {
        sikkerlogg.info("Ekspederer inntektsmelding med fødselsnummer: ${inntektsmelding.fnr} og orgnummer: ${inntektsmelding.orgnummer}")
        return flaggFlereInntektsmeldinger(inntektsmelding.melding, antallInntektsmeldingMottatt)
    }

    private fun flaggFlereInntektsmeldinger(melding: Melding.Inntektsmelding, antallInntektsmeldinger: Int) =
        BeriketMelding(
            json = (objectmapper.readTree(melding.rapidhendelse) as ObjectNode).apply {
                put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)
            }.toString()
        )
}
