package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*

data class Meldingsdetaljer(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
) {
    constructor(
        type: String,
        fnr: String,
        eksternDokumentId: UUID,
        rapportertDato: LocalDateTime,
        duplikatnøkkel: List<String>,
        jsonBody: String,
    ) : this(type, fnr, eksternDokumentId, rapportertDato, duplikatnøkkel.joinToString(separator = "").sha512(), fjernInternIdFraJson(jsonBody))

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

        private fun søknad(type: String, fnr: String, eksternDokumentId: UUID, søknadId: UUID, søknadStatus: String, rapportertDato: LocalDateTime, jsonBody: String): Meldingsdetaljer {
            return Meldingsdetaljer(
                type = type,
                fnr = fnr,
                eksternDokumentId = eksternDokumentId,
                rapportertDato = rapportertDato,
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
                rapportertDato = packet["opprettet"].asLocalDateTime(),
                jsonBody = packet.toJson()
            )
        private fun sendtSøknad(type: String, rapportetDatoFelt: String, packet: JsonMessage) =
            søknad(
                type = type,
                fnr = packet["fnr"].asText(),
                eksternDokumentId = packet["id"].asText().toUUID(),
                søknadId = packet["id"].asText().toUUID(),
                søknadStatus = packet["status"].asText(),
                rapportertDato = packet[rapportetDatoFelt].asLocalDateTime(),
                jsonBody = packet.toJson()
            )

        fun nySøknad(packet: JsonMessage) = nySøknad("ny_søknad", packet)
        fun nySøknadFrilans(packet: JsonMessage) = nySøknad("ny_søknad_frilans", packet)
        fun nySøknadSelvstendig(packet: JsonMessage) = nySøknad("ny_søknad_selvstendig", packet)
        fun nySøknadArbeidsledig(packet: JsonMessage) = nySøknad("ny_søknad_arbeidsledig", packet)

        fun sendtSøknadArbeidsgiver(packet: JsonMessage) = sendtSøknad("sendt_søknad_arbeidsgiver", "sendtArbeidsgiver", packet)
        fun sendtSøknadNav(packet: JsonMessage) = sendtSøknad("sendt_søknad_nav", "sendtNav", packet)
        fun sendtSøknadFrilans(packet: JsonMessage) = sendtSøknad("sendt_søknad_frilans", "sendtNav", packet)
        fun sendtSøknadSelvstendig(packet: JsonMessage) = sendtSøknad("sendt_søknad_selvstendig", "sendtNav", packet)
        fun sendtSøknadArbeidsledig(packet: JsonMessage) = sendtSøknad("sendt_søknad_arbeidsledig", "sendtNav", packet)

        fun avbruttSøknad(packet: JsonMessage) = sendtSøknad("avbrutt_søknad", "opprettet", packet)
        fun avbruttSøknadArbeidsledig(packet: JsonMessage) = sendtSøknad("avbrutt_arbeidsledig_søknad", "opprettet", packet)
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
        put("@opprettet", meldingsdetaljer.rapportertDato.toString())
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
}
