package no.nav.helse.spedisjon.async

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

class DokumentAliasProducer(val topic: String, val producer: KafkaProducer<String, String>) {
    private companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)
        private val sikkerlogg: Logger = LoggerFactory.getLogger("tjenestekall")
    }

    fun send(melding: Melding) {
        val dokumenttype = melding.dokumenttypeOrNull() ?: return
        val hendelsenavn = melding.meldingsdetaljer.type
        val internDokumentId = melding.internId
        val eksternDokumentId = melding.meldingsdetaljer.eksternDokumentId
        val rapportertDato = melding.meldingsdetaljer.rapportertDato
        val dokumentaliasmelding = dokumentalias(dokumenttype, hendelsenavn, internDokumentId, eksternDokumentId, rapportertDato)
        producer.send(ProducerRecord(topic, melding.meldingsdetaljer.fnr, dokumentaliasmelding))

        if (melding !is Melding.SendtSøknad) return
        // søknader knyttes til sykmeldingen også
        val sykmeldingId = melding.sykmeldingId
        val sykmeldingmelding = dokumentalias(Dokumenttype.SYKMELDING, hendelsenavn, internDokumentId, sykmeldingId, rapportertDato)
        producer.send(ProducerRecord(topic, melding.meldingsdetaljer.fnr, sykmeldingmelding))
    }

    private fun Melding.dokumenttypeOrNull() = when (this) {
        is Melding.Inntektsmelding -> Dokumenttype.INNTEKTSMELDING
        is Melding.NySøknad -> Dokumenttype.SYKMELDING
        is Melding.SendtSøknad -> Dokumenttype.SØKNAD
        else -> {
            logger.info("lager ikke dokument-alias-melding for ${this::class.simpleName}")
            sikkerlogg.info("lager ikke dokument-alias-melding for ${this::class.simpleName}")
            null
        }
    }

    private enum class Dokumenttype {
        INNTEKTSMELDING, SYKMELDING, SØKNAD
    }

    @Language("JSON")
    private fun dokumentalias(dokumenttype: Dokumenttype, hendelsenavn: String, internDokumentId: UUID, eksternDokumentId: UUID, opprettet: LocalDateTime) = """
    {
        "@event_name": "dokument_alias",
        "@id": "${UUID.randomUUID()}",
        "dokumenttype": "${dokumenttype.name.uppercase()}",
        "hendelsenavn": "$hendelsenavn",
        "intern_dokument_id": "$internDokumentId",
        "@opprettet": "$opprettet",
        "ekstern_dokument_id": "$eksternDokumentId"
    }"""
}