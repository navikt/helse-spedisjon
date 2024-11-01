package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.speed.SpeedClient
import kotliquery.Session
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.spedisjon.Melding.Inntektsmelding
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingDao(dataSource: DataSource): AbstractDao(dataSource) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

    fun leggInn(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime = LocalDateTime.now()): Boolean {
        sikkerlogg.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding, ønsketPublisert, mottatt).also {
            if (!it) sikkerlogg.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    private fun markerSomEkspedert(session: Session, melding: Melding.Inntektsmelding) {
        sikkerlogg.info("markerer inntektsmelding med duplikatkontroll ${melding.duplikatkontroll()} som ekspedert")
        """UPDATE inntektsmelding SET ekspedert = :ekspedert WHERE duplikatkontroll = :duplikatkontroll"""
            .update(session, mapOf("ekspedert" to LocalDateTime.now(), "duplikatkontroll" to melding.duplikatkontroll()))
    }

    fun hentSendeklareMeldinger(inntektsmeldingTimeoutSekunder: Long, onEach: (SendeklarInntektsmelding, Int) -> Unit) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val stmt = """SELECT i.fnr, i.orgnummer, i.mottatt, m.data
            FROM inntektsmelding i 
            JOIN melding m ON i.duplikatkontroll = m.duplikatkontroll 
            WHERE i.ekspedert IS NULL AND i.timeout < :timeout
            LIMIT 500
            FOR UPDATE
            SKIP LOCKED"""

            stmt
                .listQuery(session, mapOf("timeout" to LocalDateTime.now())) { row ->
                    SendeklarInntektsmelding(
                        fnr = row.string("fnr"),
                        orgnummer = row.string("orgnummer"),
                        melding = Inntektsmelding.lagInntektsmelding(row.string("data")),
                        mottatt = row.localDateTime("mottatt")
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

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, arbeidsforhold_id, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :arbeidsforhold_id, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "arbeidsforhold_id" to melding.arbeidsforholdId(),
                            "mottatt" to mottatt,
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1
}
