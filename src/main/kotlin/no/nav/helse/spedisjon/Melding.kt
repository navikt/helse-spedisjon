package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*

abstract class Melding(protected val packet: JsonMessage) {
    private val id = UUID.randomUUID()

    abstract val type: String
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

    fun jsonNode(): JsonNode = jacksonObjectMapper().readTree(json())

    internal companion object {
        internal fun String.sha512(): String {
            return MessageDigest
                .getInstance("SHA-512")
                .digest(this.toByteArray())
                .joinToString("") { "%02x".format(it) }
        }

        fun les(type: String, data: String): Melding? = when (type) {
            "inntektsmelding" -> Inntektsmelding.lagInntektsmelding(data)
            "ny_søknad" -> NySøknad.lagNySøknad(data)
            "sendt_søknad_arbeidsgiver" -> SendtSøknadArbeidsgiver.lagSendtSøknadArbeidsgiver(data)
            "sendt_søknad_nav" -> SendtSøknadNav.lagSendtSøknadNav(data)
            else -> null
        }
    }

    class NySøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagNySøknad(data: String) : NySøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data)).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return NySøknad(jsonMessage)
            }
        }
    }

    class SendtSøknadArbeidsgiver(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_arbeidsgiver"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtArbeidsgiver"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagSendtSøknadArbeidsgiver(data: String) : SendtSøknadArbeidsgiver {
                val jsonMessage = JsonMessage(data, MessageProblems(data)).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtArbeidsgiver")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtSøknadArbeidsgiver(jsonMessage)
            }
        }
    }

    class SendtSøknadNav(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_nav"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagSendtSøknadNav(data: String): SendtSøknadNav {
                val jsonMessage = JsonMessage(data, MessageProblems(data)).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtNav")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtSøknadNav(jsonMessage)
            }
        }
    }

    class Inntektsmelding(packet: JsonMessage) : Melding(packet) {
        override val type = "inntektsmelding"
        override fun fødselsnummer(): String = packet["arbeidstakerFnr"].asText().toString()
        override fun rapportertDato() = packet["mottattDato"].asLocalDateTime()
        override fun duplikatnøkkel(): String = packet["arkivreferanse"].asText()
        fun orgnummer(): String = packet["virksomhetsnummer"].asText()

        companion object {
            fun lagInntektsmelding(data: String) : Inntektsmelding {
                val jsonMessage = JsonMessage(data, MessageProblems(data)).also {
                    it.interestedIn("arbeidstakerFnr")
                    it.interestedIn("virksomhetsnummer")
                    it.interestedIn("mottattDato")
                    it.interestedIn("arkivreferanse")
                }
                return Inntektsmelding(jsonMessage)
            }
        }
    }
}
