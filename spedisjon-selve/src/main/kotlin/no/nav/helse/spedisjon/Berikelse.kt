package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import java.time.LocalDate

class Berikelse(
    private val fødselsdato: LocalDate,
    private val dødsdato: LocalDate?,
    private val aktørId: String,
    private val historiskeFolkeregisteridenter: List<String>
) {

    internal fun berik(melding: Melding): JsonMessage {
        melding.packet["fødselsdato"] = fødselsdato.toString()
        if (dødsdato != null) melding.packet["dødsdato"] = dødsdato
        melding.packet["historiskeFolkeregisteridenter"] = historiskeFolkeregisteridenter
        if (melding is Melding.Inntektsmelding) return melding.packet // beriker ikke inntektsmelding med aktørId
        melding.packet["aktorId"] = aktørId
        return melding.packet
    }
}