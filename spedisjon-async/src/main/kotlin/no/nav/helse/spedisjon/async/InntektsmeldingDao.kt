package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import kotliquery.Session
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.spedisjon.async.Melding.Inntektsmelding
import no.nav.helse.spedisjon.async.SendeklarInntektsmelding.Companion.sorter
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class InntektsmeldingDao(
    private val meldingtjeneste: Meldingtjeneste,
    dataSource: DataSource
): AbstractDao(dataSource) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()
    }

    fun leggInn(melding: Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime): Boolean {
        sikkerlogg.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding, ønsketPublisert, mottatt).also {
            if (!it) sikkerlogg.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.meldingsdetaljer.duplikatkontroll), melding.meldingsdetaljer.jsonBody)
        }
    }

    private fun markerSomEkspedert(session: Session, melding: Inntektsmelding) {
        sikkerlogg.info("markerer inntektsmelding med duplikatkontroll ${melding.meldingsdetaljer.duplikatkontroll} som ekspedert")
        """UPDATE inntektsmelding SET ekspedert = :ekspedert WHERE duplikatkontroll = :duplikatkontroll"""
            .update(session, mapOf("ekspedert" to LocalDateTime.now(), "duplikatkontroll" to melding.meldingsdetaljer.duplikatkontroll))
    }

    private data class SendeklarInntektsmeldingDto(
        val fnr: String,
        val orgnummer: String,
        val internDokumentId: UUID,
        val arbeidsforholdId: String?,
        val mottatt: LocalDateTime,
        val duplikatkontroll: String
    )
    fun hentSendeklareMeldinger(inntektsmeldingTimeoutSekunder: Long, timeout: LocalDateTime = LocalDateTime.now(), onEach: (SendeklarInntektsmelding, Int) -> Unit) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val stmt = """SELECT fnr, orgnummer, arbeidsforhold_id, mottatt, duplikatkontroll, intern_dokument_id
            FROM inntektsmelding 
            WHERE ekspedert IS NULL AND timeout < :timeout
            LIMIT 500
            FOR UPDATE
            SKIP LOCKED"""

            val sendeklareInntektsmeldinger = stmt
                .listQuery(session, mapOf("timeout" to timeout)) { row ->
                    SendeklarInntektsmeldingDto(
                        fnr = row.string("fnr"),
                        orgnummer = row.string("orgnummer"),
                        internDokumentId = row.uuid("intern_dokument_id"),
                        arbeidsforholdId = row.stringOrNull("arbeidsforhold_id"),
                        mottatt = row.localDateTime("mottatt"),
                        duplikatkontroll = row.string("duplikatkontroll")
                    )
                }

            //if (sendeklareInntektsmeldinger.isEmpty()) return@use

            val inntektsmeldinger = meldingtjeneste.hentMeldinger(sendeklareInntektsmeldinger.map { it.internDokumentId })

            sendeklareInntektsmeldinger.map { dto ->
                val inntektsmelding = inntektsmeldinger.meldinger.first { it.internDokumentId == dto.internDokumentId }
                SendeklarInntektsmelding(
                    fnr = dto.fnr,
                    orgnummer = dto.orgnummer,
                    melding = Inntektsmelding(
                        internId = dto.internDokumentId,
                        orgnummer = dto.orgnummer,
                        arbeidsforholdId = dto.arbeidsforholdId,
                        meldingsdetaljer = Meldingsdetaljer(
                            type = "inntektsmelding",
                            fnr = dto.fnr,
                            eksternDokumentId = inntektsmelding.eksternDokumentId,
                            rapportertDato = objectMapper.readTree(inntektsmelding.jsonBody).path("mottattDato")
                                .asLocalDateTime(),
                            duplikatkontroll = dto.duplikatkontroll,
                            jsonBody = inntektsmelding.jsonBody
                        )
                    ),
                    mottatt = dto.mottatt
                )
            }
                .sorter()
                .also {
                    sikkerlogg.info("Ekspederer ${it.size} fra databasen")
                    logg.info("Ekspederer ${it.size} fra databasen")
                }
                .onEach {
                    val antallInntektsmeldingMottatt = tellInntektsmeldinger(session, it.fnr, it.orgnummer, it.mottatt.minusSeconds(inntektsmeldingTimeoutSekunder))
                    onEach(it, antallInntektsmeldingMottatt)
                    markerSomEkspedert(session, it.melding)
                }
        }
    }

    private fun tellInntektsmeldinger(session: Session, fnr: String, orgnummer: String, tattImotEtter: LocalDateTime): Int {
        return """SELECT COUNT (1)
            FROM inntektsmelding 
            WHERE mottatt >= :tattImotEtter AND fnr = :fnr AND orgnummer = :orgnummer AND arbeidsforhold_id IS NOT NULL
        """.singleQuery(session, mapOf("tattImotEtter" to tattImotEtter, "fnr" to fnr, "orgnummer" to orgnummer))
        { row -> row.int("count") }!!
    }

    private fun leggInnUtenDuplikat(melding: Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, intern_dokument_id, arbeidsforhold_id, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :internDokumentId, :arbeidsforhold_id, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.meldingsdetaljer.fnr,
                            "orgnummer" to melding.orgnummer,
                            "internDokumentId" to melding.internId,
                            "arbeidsforhold_id" to melding.arbeidsforholdId,
                            "mottatt" to mottatt,
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.meldingsdetaljer.duplikatkontroll)) == 1
}
