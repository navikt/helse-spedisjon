package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*

abstract class Melding {
    abstract val type: String
    abstract fun fødselsnummer(): String
    abstract fun rapportertDato(): LocalDateTime
    protected abstract fun duplikatnøkkel(): String
    fun duplikatkontroll() = duplikatnøkkel().sha512()
    abstract fun json(): String

    private companion object {
        private fun String.sha512(): String {
            return MessageDigest
                .getInstance("SHA-512")
                .digest((this).toByteArray())
                .fold("", { str, it -> str + "%02x".format(it) })
        }
    }

    class NySøknad(private val packet: JsonMessage) : Melding() {
        override val type = "ny_søknad"
        override fun fødselsnummer() = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText() + packet["opprettet"].asText()
        override fun json(): String {
            packet["@event_name"] = type
            packet["@id"] = UUID.randomUUID()
            packet["@opprettet"] = rapportertDato()
            return packet.toJson()
        }
    }

    class SendtSøknad(private val packet: JsonMessage) : Melding() {
        override val type = "sendt_søknad"
        override fun fødselsnummer() = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText() + packet["sendtNav"].asText()
        override fun json(): String {
            packet["@event_name"] = type
            packet["@id"] = UUID.randomUUID()
            packet["@opprettet"] = rapportertDato()
            return packet.toJson()
        }
    }

    class Inntektsmelding(private val packet: JsonMessage) : Melding() {
        override val type = "inntektsmelding"
        override fun fødselsnummer() = packet["arbeidstakerFnr"].asText()
        override fun rapportertDato() = packet["mottattDato"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["arkivreferanse"].asText()
        override fun json(): String {
            packet["@event_name"] = type
            packet["@id"] = UUID.randomUUID()
            packet["@opprettet"] = rapportertDato()
            return packet.toJson()
        }
    }
}