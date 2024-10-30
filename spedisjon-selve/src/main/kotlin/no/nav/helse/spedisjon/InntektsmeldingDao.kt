package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.PersonResponse.Adressebeskyttelse
import com.github.navikt.tbd_libs.speed.SpeedClient
import kotliquery.TransactionalSession
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.withMDC
import no.nav.helse.spedisjon.Melding.Inntektsmelding
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class InntektsmeldingDao(dataSource: DataSource): AbstractDao(dataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
    }

    fun leggInn(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime = LocalDateTime.now()): Boolean {
        log.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding, ønsketPublisert, mottatt).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    fun markerSomEkspedert(melding: Melding.Inntektsmelding, session: TransactionalSession) {
        log.info("markerer inntektsmelding med duplikatkontroll ${melding.duplikatkontroll()} som ekspedert")
        """UPDATE inntektsmelding SET ekspedert = :ekspedert WHERE duplikatkontroll = :duplikatkontroll"""
            .update(session, mapOf("ekspedert" to LocalDateTime.now(), "duplikatkontroll" to melding.duplikatkontroll()))
    }

    fun hentSendeklareMeldinger(session: TransactionalSession, speedClient: SpeedClient): List<SendeklarInntektsmelding> {
        return """SELECT i.fnr, i.orgnummer, i.mottatt, m.data
            FROM inntektsmelding i 
            JOIN melding m ON i.duplikatkontroll = m.duplikatkontroll 
            WHERE i.ekspedert IS NULL AND i.timeout < :timeout
            FOR UPDATE
            SKIP LOCKED""".trimMargin()
            .listQuery(session, mapOf("timeout" to LocalDateTime.now()))
            { row ->
                InntektsmeldingFraDatabasen(
                    fnr = row.string("fnr"),
                    orgnummer = row.string("orgnummer"),
                    melding = Inntektsmelding.lagInntektsmelding(row.string("data")),
                    mottatt = row.localDateTime("mottatt")
                )
            }.mapNotNull {
                val callId = UUID.randomUUID().toString()
                withMDC("callId" to callId) {
                    log.info("henter berikelse for inntektsmelding")
                    val personinfo = retryBlocking { speedClient.hentPersoninfo(it.fnr, callId) }
                    val historiskeIdenter = retryBlocking { speedClient.hentHistoriskeFødselsnumre(it.fnr, callId) }
                    val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(it.fnr, callId) }

                    val støttes = personinfo.adressebeskyttelse !in setOf(Adressebeskyttelse.STRENGT_FORTROLIG, Adressebeskyttelse.STRENGT_FORTROLIG_UTLAND)
                    if (!støttes) {
                        log.info("Personen støttes ikke ${identer.aktørId}")
                        null
                    } else {
                        val berikelse = Berikelse(
                            fødselsdato = personinfo.fødselsdato,
                            dødsdato = personinfo.dødsdato,
                            aktørId = identer.aktørId,
                            historiskeFolkeregisteridenter = historiskeIdenter.fødselsnumre
                        )

                        SendeklarInntektsmelding(
                            fnr = it.fnr,
                            orgnummer = it.orgnummer,
                            originalMelding = it.melding,
                            berikelse = berikelse,
                            mottatt = it.mottatt
                        )
                    }
                }
            }
    }

    private data class InntektsmeldingFraDatabasen(
        val fnr: String,
        val orgnummer: String,
        val melding: Melding.Inntektsmelding,
        val mottatt: LocalDateTime
    )

    fun tellInntektsmeldinger(fnr: String, orgnummer: String, tattImotEtter: LocalDateTime): Int {
        return """SELECT COUNT (1)
            FROM inntektsmelding 
            WHERE mottatt >= :tattImotEtter AND fnr = :fnr AND orgnummer = :orgnummer AND arbeidsforhold_id IS NOT NULL
        """.trimMargin().singleQuery(mapOf("tattImotEtter" to tattImotEtter, "fnr" to fnr, "orgnummer" to orgnummer))
        { row -> row.int("count") }!!
    }

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, arbeidsforhold_id, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :arbeidsforhold_id, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "arbeidsforhold_id" to melding.arbeidsforholdId(),
                            "mottatt" to mottatt,
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1

    fun transactionally(f: TransactionalSession.() -> Unit) {
        sessionOf(dataSource).use {
            it.transaction { f(it) }
        }
    }

}
