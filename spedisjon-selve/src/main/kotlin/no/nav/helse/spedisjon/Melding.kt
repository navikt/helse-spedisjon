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

abstract class Melding(val packet: JsonMessage) {
    val id = UUID.randomUUID()

    abstract val type: String
    abstract val eksternDokumentId: UUID
    abstract fun fødselsnummer(): String
    abstract fun rapportertDato(): LocalDateTime
    protected abstract fun duplikatnøkkel(): String
    fun duplikatkontroll() = duplikatnøkkel().sha512()
    fun json(): String {
        packet["@event_name"] = type
        packet["@id"] = id
        packet["@opprettet"] = rapportertDato()
        return packet.toJson()
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

    class NySøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad"
        override val eksternDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
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
    class NyFrilansSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_frilans"
        override val eksternDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }
    class NySelvstendigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_selvstendig"
        override val eksternDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }
    class NyArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_arbeidsledig"
        override val eksternDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadArbeidsgiver(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_arbeidsgiver"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtArbeidsgiver"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadNav(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_nav"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtFrilansSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_frilans"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSelvstendigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_selvstendig"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_arbeidsledig"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        val sykmeldingDokumentId = packet["sykmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class AvbruttSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "avbrutt_søknad"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, ${packet["arbeidsgiver.orgnummer"].asText()}, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"
    }
    class AvbruttArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "avbrutt_arbeidsledig_søknad"
        override val eksternDokumentId = packet["id"].asText().toUUID()
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, arbeidsledig, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"
    }

    class Inntektsmelding(packet: JsonMessage) : Melding(packet) {
        override val type = "inntektsmelding"
        override val eksternDokumentId = packet["inntektsmeldingId"].asText().toUUID()
        override fun fødselsnummer(): String = packet["arbeidstakerFnr"].asText().toString()
        override fun rapportertDato() = packet["mottattDato"].asLocalDateTime()
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
                }
                return Inntektsmelding(jsonMessage)
            }
        }
    }
}
