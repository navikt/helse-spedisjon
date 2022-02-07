package db.migration

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.spedisjon.Melding.Companion.sha512
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime

class V3__test_migrering_av_duplikatkontroll() : BaseJavaMigration() {

    override fun migrate(context: Context) {
        val start = LocalDateTime.now()
        context.connection.prepareStatement(UPDATE).use { updateStatement ->
            context.connection.createStatement().use { selectStatement ->
                selectStatement.executeQuery(SELECT).use { resultSet ->
                    var batchNummer = 1
                    var antall = 0L
                    var batchStart = LocalDateTime.now()
                    while (resultSet.next()) {
                        val data = objectMapper.readTree(resultSet.getString("data"))
                        val duplikatkontroll = data["id"].asText() + data["status"].asText()
                        val sha512 = duplikatkontroll.sha512()
                        updateStatement.setString(1, sha512)
                        updateStatement.setString(2, sha512)
                        updateStatement.setLong(3, resultSet.getLong("id"))
                        updateStatement.addBatch()
                        antall += 1
                        if (antall == batchSize) {
                            val batchEnd = LocalDateTime.now()
                            updateStatement.executeBatch()
                            updateStatement.clearBatch()
                            val queryEnd = LocalDateTime.now()
                            val diff = Duration.between(batchStart, queryEnd).toSeconds()
                            val perSekund = (batchSize / diff.toDouble() * 100).toInt() / 100.0
                            val diffQuery = Duration.between(batchEnd, queryEnd).toSeconds()
                            log.info("Utført batch $batchNummer på $diff sekunder ($perSekund rader per sekund). Spørringen tok $diffQuery sekunder")
                            antall = 0
                            batchNummer += 1
                            batchStart = LocalDateTime.now()
                        }
                    }
                    if (antall > 0) {
                        updateStatement.executeBatch()
                        updateStatement.clearBatch()
                    }
                }
            }
        }
        val diff = Duration.between(start, LocalDateTime.now()).toSeconds()
        log.info("Migreringen tok $diff sekunder")
    }

    private companion object {
        private const val batchSize = 10000L
        private val log = LoggerFactory.getLogger(V3__test_migrering_av_duplikatkontroll::class.java)
        @Language("PostgreSQL")
        private const val SELECT =
            "SELECT id, data FROM melding WHERE type = 'ny_søknad' AND tmp_duplikatkontroll IS NULL AND tmp_slett = false ORDER BY opprettet LIMIT 100000"

        @Language("PostgreSQL")
        private const val UPDATE =
            "UPDATE melding SET tmp_duplikatkontroll = ?, tmp_slett = (SELECT EXISTS (SELECT 1 FROM melding WHERE tmp_duplikatkontroll = ?)) WHERE id = ?"

        private val objectMapper = jacksonObjectMapper()
    }
}