package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*

data class Meldingsdetaljer(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
) {
    constructor(
        type: String,
        fnr: String,
        eksternDokumentId: UUID,
        duplikatnøkkel: List<String>,
        jsonBody: String,
    ) : this(type, fnr, eksternDokumentId, duplikatnøkkel.joinToString(separator = "").sha512(), fjernInternIdFraJson(jsonBody))

    companion object {
        private val objectmapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

        private fun fjernInternIdFraJson(jsonBody: String) = (objectmapper.readTree(jsonBody) as ObjectNode).apply {
            remove("@id")
            remove("@opprettet")
            remove("system_read_count")
            remove("system_participating_services")
        }.toString()

        internal fun String.sha512(): String =
            MessageDigest
                .getInstance("SHA-512")
                .digest(this.toByteArray())
                .joinToString("") { "%02x".format(it) }

        private fun søknad(type: String, fnr: String, eksternDokumentId: UUID, søknadId: UUID, søknadStatus: String, jsonBody: String): Meldingsdetaljer {
            return Meldingsdetaljer(
                type = type,
                fnr = fnr,
                eksternDokumentId = eksternDokumentId,
                duplikatnøkkel = listOf(søknadId.toString(), søknadStatus),
                jsonBody = jsonBody
            )
        }

        private fun nySøknad(type: String, packet: JsonMessage) =
            søknad(
                type = type,
                fnr = packet["fnr"].asText(),
                eksternDokumentId = packet["sykmeldingId"].asText().toUUID(),
                søknadId = packet["id"].asText().toUUID(),
                søknadStatus = packet["status"].asText(),
                jsonBody = packet.toJson()
            )
        private fun sendtSøknad(type: String, packet: JsonMessage) =
            søknad(
                type = type,
                fnr = packet["fnr"].asText(),
                eksternDokumentId = packet["id"].asText().toUUID(),
                søknadId = packet["id"].asText().toUUID(),
                søknadStatus = packet["status"].asText(),
                jsonBody = packet.toJson()
            )

        fun nySøknad(packet: JsonMessage) = nySøknad("ny_søknad", packet)
        fun nySøknadFrilans(packet: JsonMessage) = nySøknad("ny_søknad_frilans", packet)
        fun nySøknadSelvstendig(packet: JsonMessage) = nySøknad("ny_søknad_selvstendig", packet)
        fun nySøknadArbeidsledig(packet: JsonMessage) = nySøknad("ny_søknad_arbeidsledig", packet)

        fun sendtSøknadArbeidsgiver(packet: JsonMessage) = sendtSøknad("sendt_søknad_arbeidsgiver", packet)
        fun sendtSøknadNav(packet: JsonMessage) = sendtSøknad("sendt_søknad_nav", packet)
        fun sendtSøknadFrilans(packet: JsonMessage) = sendtSøknad("sendt_søknad_frilans", packet)
        fun sendtSøknadSelvstendig(packet: JsonMessage) = sendtSøknad("sendt_søknad_selvstendig", packet)
        fun sendtSøknadArbeidsledig(packet: JsonMessage) = sendtSøknad("sendt_søknad_arbeidsledig", packet)

        fun avbruttSøknad(packet: JsonMessage) = sendtSøknad("avbrutt_søknad", packet)
        fun avbruttSøknadArbeidsledig(packet: JsonMessage) = sendtSøknad("avbrutt_arbeidsledig_søknad", packet)
        fun avbruttSøknadSelvstendig(packet: JsonMessage) = sendtSøknad("avbrutt_selvstendig_søknad", packet)
        fun avbruttSøknadBarnepasser(packet: JsonMessage) = sendtSøknad("avbrutt_barnepasser_søknad", packet)
        fun avbruttSøknadFrilanser(packet: JsonMessage) = sendtSøknad("avbrutt_frilanser_søknad", packet)
        fun avbruttSøknadFisker(packet: JsonMessage) = sendtSøknad("avbrutt_fisker_søknad", packet)
        fun avbruttSøknadJordbruker(packet: JsonMessage) = sendtSøknad("avbrutt_jordbruker_søknad", packet)
        fun avbruttSøknadAnnet(packet: JsonMessage) = sendtSøknad("avbrutt_annet_søknad", packet)
    }
}

sealed class Melding(val internId: UUID, val meldingsdetaljer: Meldingsdetaljer) {
    private companion object {
        private val objectmapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }
    val rapidhendelse = (objectmapper.readTree(meldingsdetaljer.jsonBody) as ObjectNode).apply {
        put("@event_name", meldingsdetaljer.type)
        put("@id", internId.toString())
        put("@opprettet", LocalDateTime.now().toString())
    }.toString()

    class NySøknad(internId: UUID, meldingsdetaljer: Meldingsdetaljer) : Melding(internId, meldingsdetaljer)
    class SendtSøknad(internId: UUID, val sykmeldingId: UUID, meldingsdetaljer: Meldingsdetaljer) : Melding(internId, meldingsdetaljer)
    class AvbruttSøknad(internId: UUID, meldingsdetaljer: Meldingsdetaljer) : Melding(internId, meldingsdetaljer)
    class Inntektsmelding(
        internId: UUID,
        val orgnummer: String,
        val arbeidsforholdId: String?,
        meldingsdetaljer: Meldingsdetaljer
    ) : Melding(internId, meldingsdetaljer)
    class Arbeidsgiveropplysninger(
        internId: UUID,
        meldingsdetaljer: Meldingsdetaljer
    ) : Melding(internId, meldingsdetaljer)
}
