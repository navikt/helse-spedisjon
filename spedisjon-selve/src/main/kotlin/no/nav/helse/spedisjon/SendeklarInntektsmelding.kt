package no.nav.helse.spedisjon

import java.time.LocalDateTime

data class SendeklarInntektsmelding(
    val fnr: String,
    val orgnummer: String,
    val melding: Melding.Inntektsmelding,
    val mottatt: LocalDateTime
) {
    companion object {
        internal fun List<SendeklarInntektsmelding>.sorter() = sortedBy { it.mottatt }
    }
}