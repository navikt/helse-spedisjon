package no.nav.helse.opprydding

import javax.sql.DataSource
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf

internal class PersonRepository(private val dataSource: DataSource) {
    internal fun slett(fødselsnummer: String) {
        sessionOf(dataSource).use { session ->
            session.transaction {
                it.slettMeldinger(fødselsnummer)
                it.slettBerikelser(fødselsnummer)
                it.slettInntektsmeldinger(fødselsnummer)
            }
        }
    }

    private fun TransactionalSession.slettMeldinger(fødselsnummer: String) {
        val query = "DELETE FROM melding WHERE fnr = ?"
        run(queryOf(query, fødselsnummer).asExecute)
    }

    private fun TransactionalSession.slettBerikelser(fødselsnummer: String) {
        val query = "DELETE FROM berikelse WHERE fnr = ?"
        run(queryOf(query, fødselsnummer).asExecute)
    }

    private fun TransactionalSession.slettInntektsmeldinger(fødselsnummer: String) {
        val query = "DELETE FROM inntektsmelding WHERE fnr = ?"
        run(queryOf(query, fødselsnummer).asExecute)
    }
}
