package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
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

    internal companion object {
        internal fun String.sha512(): String {
            return MessageDigest
                .getInstance("SHA-512")
                .digest(this.toByteArray())
                .joinToString("") { "%02x".format(it) }
        }
    }

    class NySøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadArbeidsgiver(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_arbeidsgiver"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtArbeidsgiver"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class SendtSøknadNav(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_nav"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
    }

    class Inntektsmelding(packet: JsonMessage) : Melding(packet) {
        override val type = "inntektsmelding"
        override fun fødselsnummer(): String = packet["arbeidstakerFnr"].asText().toString()
        override fun rapportertDato() = packet["mottattDato"].asLocalDateTime()
        override fun duplikatnøkkel(): String = packet["arkivreferanse"].asText()
        fun orgnummer(): String = packet["virksomhetsnummer"].asText()
    }
}
