package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDate

class Berikelse(
    private val fødselsdato: LocalDate,
    private val dødsdato: LocalDate?,
    private val aktørId: String,
    private val historiskeFolkeregisteridenter: List<String>
) {
    private companion object {
        private val objectmapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    internal fun berik(melding: Melding): BeriketMelding {
        check(melding !is Melding.Inntektsmelding && melding !is Melding.NavNoInntektsmelding) {
            "inntektsmeldinger trenger ikke berikelse"
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

data class BeriketMelding(val json: String)
