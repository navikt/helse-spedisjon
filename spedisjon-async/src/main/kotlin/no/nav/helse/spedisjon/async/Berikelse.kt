package no.nav.helse.spedisjon.async

import java.time.LocalDate
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.kotlin.jacksonObjectMapper

class Berikelse(
    internal val fødselsdato: LocalDate,
    private val dødsdato: LocalDate?,
    private val aktørId: String,
    private val historiskeFolkeregisteridenter: List<String>
) {
    private companion object {
        private val objectmapper = jacksonObjectMapper()
    }

    internal fun berik(melding: Melding): BeriketMelding {
        check(melding !is Melding.Inntektsmelding && melding !is Melding.Arbeidsgiveropplysninger) {
            "inntektsmeldinger & arbeidsgiveropplysninger trenger ikke berikelse"
        }
        val packet = objectmapper.readTree(melding.rapidhendelse) as ObjectNode
        packet.put("fødselsdato", fødselsdato.toString())
        if (dødsdato != null) packet.put("dødsdato", dødsdato.toString())
        packet.withArray("historiskeFolkeregisteridenter").apply {
            historiskeFolkeregisteridenter.forEach { add(it) }
        }
        packet.put("aktorId", aktørId)
        return BeriketMelding(packet.toString())
    }
}

data class BeriketMelding(
    val json: String
)
