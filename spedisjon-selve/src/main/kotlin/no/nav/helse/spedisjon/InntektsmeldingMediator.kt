package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingMediator (
    dataSource: DataSource,
    private val meldingDao: MeldingDao = MeldingDao(dataSource),
    private val  inntektsmeldingDao: InntektsmeldingDao = InntektsmeldingDao(dataSource),
    private val berikelseDao: BerikelseDao = BerikelseDao(dataSource),
    private val meldingMediator: MeldingMediator = MeldingMediator(meldingDao, berikelseDao),
    private val inntektsmeldingTimeoutSekunder: Long = 1
    ) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

    fun lagreInntektsmelding(inntektsmelding: Melding.Inntektsmelding, messageContext: MessageContext) {
        meldingMediator.onMelding(inntektsmelding, messageContext)
        if (!inntektsmeldingDao.leggInn(inntektsmelding, LocalDateTime.now().plusSeconds(inntektsmeldingTimeoutSekunder))) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
    }

    fun ekspeder(messageContext: MessageContext){
        inntektsmeldingDao.transactionally {
            val sendeklareInntektsmeldinger = inntektsmeldingDao.hentSendeklareMeldinger(this).sorter()
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
