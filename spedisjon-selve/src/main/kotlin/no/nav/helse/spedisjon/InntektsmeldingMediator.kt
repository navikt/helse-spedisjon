package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.speed.SpeedClient
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
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

        if (System.getenv("NAIS_CLUSTER") == "dev-gcp") {
            logg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            sikkerlogg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            ekspeder(messageContext)
        }
    }

    fun ekspeder(messageContext: MessageContext) {
        inntektsmeldingDao.transactionally {
            val sendeklareInntektsmeldinger = inntektsmeldingDao.hentSendeklareMeldinger(this, speedClient).sorter()
            sikkerlogg.info("Ekspederer ${sendeklareInntektsmeldinger.size} fra databasen")
            logg.info("Ekspederer ${sendeklareInntektsmeldinger.size} fra databasen")
            sendeklareInntektsmeldinger.forEach {
                it.send(inntektsmeldingDao, messageContext, inntektsmeldingTimeoutSekunder, this)
            }
        }
    }

    fun onRiverError(error: String) {
        meldingMediator.onRiverError(error)
    }
}
