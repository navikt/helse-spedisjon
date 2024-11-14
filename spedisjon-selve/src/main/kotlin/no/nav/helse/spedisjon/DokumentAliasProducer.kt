package no.nav.helse.spedisjon

import no.nav.helse.spedisjon.Melding.Inntektsmelding
import no.nav.helse.spedisjon.Melding.NyArbeidsledigSøknad
import no.nav.helse.spedisjon.Melding.NyFrilansSøknad
import no.nav.helse.spedisjon.Melding.NySelvstendigSøknad
import no.nav.helse.spedisjon.Melding.NySøknad
import no.nav.helse.spedisjon.Melding.SendtArbeidsledigSøknad
import no.nav.helse.spedisjon.Melding.SendtFrilansSøknad
import no.nav.helse.spedisjon.Melding.SendtSelvstendigSøknad
import no.nav.helse.spedisjon.Melding.SendtSøknadArbeidsgiver
import no.nav.helse.spedisjon.Melding.SendtSøknadNav
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.UUID

class DokumentAliasProducer(val topic: String, val producer: KafkaProducer<String, String>) {
    private companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)
        private val sikkerlogg: Logger = LoggerFactory.getLogger("tjenestekall")
    }

    fun send(melding: Melding) {
        val dokumenttype = melding.dokumenttypeOrNull() ?: return
        val hendelsenavn = melding.meldingsdetaljer.type
        val id = melding.meldingsdetaljer.id
        val eksternDokumentId = melding.meldingsdetaljer.eksternDokumentId
        val rapportertDato = melding.meldingsdetaljer.rapportertDato
        val dokumentaliasmelding = dokumentalias(dokumenttype, hendelsenavn, id, eksternDokumentId, rapportertDato)
        producer.send(ProducerRecord(topic, melding.fødselsnummer(), dokumentaliasmelding))

        when (melding) {
            is SendtSøknadArbeidsgiver,
            is SendtSøknadNav,
            is SendtFrilansSøknad,
            is SendtSelvstendigSøknad,
            is SendtArbeidsledigSøknad -> {
                // søknader knyttes til sykmeldingen også
                val sykmeldingId = when (melding) {
                    is SendtSøknadArbeidsgiver -> melding.sykmeldingDokumentId
                    is SendtSøknadNav -> melding.sykmeldingDokumentId
                    is SendtFrilansSøknad -> melding.sykmeldingDokumentId
                    is SendtSelvstendigSøknad -> melding.sykmeldingDokumentId
                    is SendtArbeidsledigSøknad -> melding.sykmeldingDokumentId
                    else -> error("sykmeldingId må være på en søknad")
                }
                val dokumentaliasmelding = dokumentalias(Dokumenttype.SYKMELDING, hendelsenavn, id, sykmeldingId, rapportertDato)
                producer.send(ProducerRecord(topic, melding.fødselsnummer(), dokumentaliasmelding))
            }
        }
    }

    private fun Melding.dokumenttypeOrNull() = when (this) {
        is Inntektsmelding -> Dokumenttype.INNTEKTSMELDING

        is NySøknad,
        is NyFrilansSøknad,
        is NySelvstendigSøknad,
        is NyArbeidsledigSøknad -> Dokumenttype.SYKMELDING

        is SendtSøknadArbeidsgiver,
        is SendtSøknadNav,
        is SendtFrilansSøknad,
        is SendtSelvstendigSøknad,
        is SendtArbeidsledigSøknad -> Dokumenttype.SØKNAD

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