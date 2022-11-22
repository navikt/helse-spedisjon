package no.nav.helse.spedisjon

import java.time.LocalDateTime

internal class InntektsmeldingMediator(
    private val meldingDao: MeldingDao,
    private val inntektsmeldingDao: InntektsmeldingDao
    ) {

    fun lagreInntektsmelding(inntektsmelding: Melding.Inntektsmelding) {
        if (!meldingDao.leggInn(inntektsmelding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        if (!inntektsmeldingDao.leggInn(inntektsmelding, LocalDateTime.now().plusMinutes(5))) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
    }


}
