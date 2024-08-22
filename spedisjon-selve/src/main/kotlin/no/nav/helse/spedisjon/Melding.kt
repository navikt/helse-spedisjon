package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.asLocalDate
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
        private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        internal fun String.sha512(): String {
            return MessageDigest
                .getInstance("SHA-512")
                .digest(this.toByteArray())
                .joinToString("") { "%02x".format(it) }
        }

        fun les(type: String, data: String): Melding? = when (type) {
            "inntektsmelding" -> Inntektsmelding.lagInntektsmelding(data)
            "ny_søknad" -> NySøknad.lagNySøknad(data)
            "ny_søknad_frilans" -> NyFrilansSøknad.lagNyFrilansSøknad(data)
            "ny_søknad_selvstendig" -> NySelvstendigSøknad.lagNySelvstendigSøknad(data)
            "ny_søknad_arbeidsledig" -> NyArbeidsledigSøknad.lagNyArbeidsledigSøknad(data)
            "sendt_søknad_arbeidsgiver" -> SendtSøknadArbeidsgiver.lagSendtSøknadArbeidsgiver(data)
            "sendt_søknad_nav" -> SendtSøknadNav.lagSendtSøknadNav(data)
            "sendt_søknad_frilans" -> SendtFrilansSøknad.lagSendtFrilansSøknad(data)
            "sendt_søknad_selvstendig" -> SendtSelvstendigSøknad.lagSendtSelvstendigSøknad(data)
            "sendt_søknad_arbeidsledig" -> SendtArbeidsledigSøknad.lagSendtArbeidsledigSøknad(data)
            "avbrutt_søknad" -> AvbruttSøknad.lagAvbruttSøknad(data)
            "avbrutt_arbeidsledig_søknad" -> AvbruttArbeidsledigSøknad.lagAvbruttSøknad(data)
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
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return NySøknad(jsonMessage)
            }
        }
    }
    class NyFrilansSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_frilans"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagNyFrilansSøknad(data: String) : NyFrilansSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return NyFrilansSøknad(jsonMessage)
            }
        }
    }
    class NySelvstendigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_selvstendig"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagNySelvstendigSøknad(data: String) : NySelvstendigSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return NySelvstendigSøknad(jsonMessage)
            }
        }
    }
    class NyArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "ny_søknad_arbeidsledig"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagNyArbeidsledigSøknad(data: String) : NyArbeidsledigSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return NyArbeidsledigSøknad(jsonMessage)
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
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
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
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtNav")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtSøknadNav(jsonMessage)
            }
        }
    }

    class SendtFrilansSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_frilans"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagSendtFrilansSøknad(data: String): SendtFrilansSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtNav")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtFrilansSøknad(jsonMessage)
            }
        }
    }

    class SendtSelvstendigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_selvstendig"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagSendtSelvstendigSøknad(data: String): SendtSelvstendigSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtNav")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtSelvstendigSøknad(jsonMessage)
            }
        }
    }


    class SendtArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "sendt_søknad_arbeidsledig"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["sendtNav"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()

        companion object {
            fun lagSendtArbeidsledigSøknad(data: String): SendtArbeidsledigSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("sendtNav")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return SendtArbeidsledigSøknad(jsonMessage)
            }
        }
    }

    class AvbruttSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "avbrutt_søknad"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, ${packet["arbeidsgiver.orgnummer"].asText()}, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"

        companion object {
            fun lagAvbruttSøknad(data: String) : AvbruttSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return AvbruttSøknad(jsonMessage)
            }
        }

    }
    class AvbruttArbeidsledigSøknad(packet: JsonMessage) : Melding(packet) {
        override val type = "avbrutt_arbeidsledig_søknad"
        override fun fødselsnummer(): String = packet["fnr"].asText()
        override fun rapportertDato() = packet["opprettet"].asLocalDateTime()
        override fun duplikatnøkkel() = packet["id"].asText() + packet["status"].asText()
        override fun toString(): String = "${fødselsnummer()}, arbeidsledig, ${packet["fom"].asLocalDate()} til ${packet["tom"].asLocalDate()}"

        companion object {
            fun lagAvbruttSøknad(data: String) : AvbruttArbeidsledigSøknad {
                val jsonMessage = JsonMessage(data, MessageProblems(data), registry).also {
                    it.interestedIn("fnr")
                    it.interestedIn("opprettet")
                    it.interestedIn("id")
                    it.interestedIn("status")
                }
                return AvbruttArbeidsledigSøknad(jsonMessage)
            }
        }

    }

    class Inntektsmelding(packet: JsonMessage) : Melding(packet) {
        override val type = "inntektsmelding"
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
                    it.interestedIn("mottattDato")
                    it.interestedIn("arkivreferanse")
                }
                return Inntektsmelding(jsonMessage)
            }
        }
    }
}
