package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageContext
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingMediator (
    dataSource: DataSource,
    private val meldingDao: MeldingDao = MeldingDao(dataSource),
    private val inntektsmeldingDao: InntektsmeldingDao = InntektsmeldingDao(dataSource),
    private val berikelseDao: BerikelseDao = BerikelseDao(dataSource),
    private val meldingMediator: MeldingMediator = MeldingMediator(meldingDao, berikelseDao)
    ) {

    fun lagreInntektsmelding(inntektsmelding: Melding.Inntektsmelding, messageContext: MessageContext) {
        meldingMediator.onMelding(inntektsmelding, messageContext)
        if (!inntektsmeldingDao.leggInn(inntektsmelding, LocalDateTime.now().plusMinutes(5))) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
    }


}
