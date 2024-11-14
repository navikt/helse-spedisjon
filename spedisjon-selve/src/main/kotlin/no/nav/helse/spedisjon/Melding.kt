package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*

data class Meldingsdetaljer(
    val id: UUID = UUID.randomUUID(),
    val type: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime
)

abstract class Melding(val packet: JsonMessage, val meldingsdetaljer: Meldingsdetaljer) {
    abstract fun fødselsnummer(): String
    protected abstract fun duplikatnøkkel(): String
    fun duplikatkontroll() = duplikatnøkkel().sha512()
    fun json(): String {
        return packet.toJson()
    }

    init {
        packet["@event_name"] = meldingsdetaljer.type
        packet["@id"] = meldingsdetaljer.id
        packet["@opprettet"] = meldingsdetaljer.rapportertDato
    }

    internal companion object {
        private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        internal fun String.sha512(): String {
            return MessageDigest
                .getInstance("SHA-512")
                .digest(this.toByteArray())
                .joinToString("") { "%02x".format(it) }
        }
    }

    class NySøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "ny_søknad", eksternDokumentId = packet["sykmeldingId"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagNySøknad(data: String) : NySøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("sykmeldingId")
                    it.interestedIn("status")
                }
                return NySøknad(jsonMessage)
            }
        }
    }
    class NyFrilansSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "ny_søknad_frilans", eksternDokumentId = packet["sykmeldingId"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }
    class NySelvstendigSøknad(packet: JsonMessage) : Melding(packet,Meldingsdetaljer(type = "ny_søknad_selvstendig", eksternDokumentId = packet["sykmeldingId"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }
    class NyArbeidsledigSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "ny_søknad_arbeidsledig", eksternDokumentId = packet["sykmeldingId"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadArbeidsgiver(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "sendt_søknad_arbeidsgiver", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["sendtArbeidsgiver"].asLocalDateTime())) {
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadNav(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "sendt_søknad_nav", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["sendtNav"].asLocalDateTime())) {
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtFrilansSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "sendt_søknad_frilans", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["sendtNav"].asLocalDateTime())) {
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSelvstendigSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "sendt_søknad_selvstendig", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["sendtNav"].asLocalDateTime())) {
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtArbeidsledigSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "sendt_søknad_arbeidsledig", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["sendtNav"].asLocalDateTime())) {
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class AvbruttSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "avbrutt_søknad", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, ${packet["arbeidsgiver.orgnummer"].asText()}, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"
    }
    class AvbruttArbeidsledigSøknad(packet: JsonMessage) : Melding(packet, Meldingsdetaljer(type = "avbrutt_arbeidsledig_søknad", eksternDokumentId = packet["id"].asText().toUUID(), rapportertDato = packet["opprettet"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, arbeidsledig, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"
    }

    class Inntektsmelding(packet: JsonMessage, id: UUID = UUID.randomUUID()) : Melding(packet, Meldingsdetaljer(id = id, type = "inntektsmelding", eksternDokumentId = packet["inntektsmeldingId"].asText().toUUID(), rapportertDato = packet["mottattDato"].asLocalDateTime())) {
        override fun fødselsnummer(): String = packet["arbeidstakerFnr"].asText().toString()
        override fun duplikatnøkkel(): String = packet["arkivreferanse"].asText()
        fun orgnummer(): String = packet["virksomhetsnummer"].asText()

        fun arbeidsforholdId(): String? = packet["arbeidsforholdId"].takeIf(JsonNode::isTextual)?.asText()

        companion object {
            fun lagInntektsmelding(data: String) : Inntektsmelding {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("arbeidstakerFnr")
                    it.interestedIn("virksomhetsnummer")
                    it.interestedIn("inntektsmeldingId")
                    it.interestedIn("mottattDato")
                    it.interestedIn("arkivreferanse")
                    it.interestedIn("@id")
                }
                return Inntektsmelding(jsonMessage, jsonMessage["@id"].asText().toUUID())
            }
        }
    }
}
