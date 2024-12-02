package no.nav.helse.spedisjon.migrering

import com.github.navikt.tbd_libs.naisful.postgres.ConnectionConfigFactory
import com.github.navikt.tbd_libs.naisful.postgres.defaultJdbcUrl
import com.github.navikt.tbd_libs.naisful.postgres.jdbcUrlWithGoogleSocketFactory
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource
import kotlin.io.path.Path
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

val log: Logger = LoggerFactory.getLogger("no.nav.helse.spedisjon.migrering.App")

fun main() {
    try {
        workMain()
        log.info("Appen er ferdig, men venter i 1 minutt med å avslutte.")
        Thread.sleep(1.minutes.toJavaDuration())
    } catch (err: Exception) {
        log.error("Alvorlig feil: ${err.message}. Jobben stopper, men venter i 5 minutter.", err)
        Thread.sleep(5.minutes.toJavaDuration())
    }
}

private fun workMain() {
    val baseConnectionConfig = HikariConfig().apply {
        jdbcUrl = defaultJdbcUrl(ConnectionConfigFactory.Env(envVarPrefix = "DATABASE"))
    }

    val flywayMigrationConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        maximumPoolSize = 2
        poolName = "flyway-migration"
        initializationFailTimeout = -1
    }
    val appConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        poolName = "spedisjon-migrering"
        maximumPoolSize = 1
    }

    val spleisConfig = HikariConfig().apply {
        jdbcUrl = jdbcUrlWithGoogleSocketFactory(System.getenv("SPLEIS_INSTANCE"), ConnectionConfigFactory.MountPath("/var/run/secrets/sql/spleis"))
        poolName = "spleis"
        maximumPoolSize = 1
    }
    val spedisjonConfig = HikariConfig().apply {
        jdbcUrl = jdbcUrlWithGoogleSocketFactory(System.getenv("SPEDISJON_INSTANCE"), ConnectionConfigFactory.MountPath("/var/run/secrets/sql/spedisjon"))
        poolName = "spedisjon"
        maximumPoolSize = 1
    }
    val spedisjonAsyncConfig = HikariConfig().apply {
        jdbcUrl = jdbcUrlForPrivateInstance("/var/run/secrets/sql/spedisjon_async", "/var/run/secrets/sql/spedisjon_async_certs")
        poolName = "spedisjon-async"
        maximumPoolSize = 1
    }

    testTilkoblinger(flywayMigrationConfig, appConfig, spleisConfig, spedisjonConfig, spedisjonAsyncConfig)
    migrateDatabase(flywayMigrationConfig)

    HikariDataSource(appConfig).use { dataSource ->
        utførMigrering(dataSource, spleisConfig, spedisjonConfig, spedisjonAsyncConfig)
    }
}

fun utførMigrering(dataSource: DataSource, spleisConfig: HikariConfig, spedisjonConfig: HikariConfig, spedisjonAsyncConfig: HikariConfig) {
    sessionOf(dataSource).use { session ->
        klargjørEllerVentPåTilgjengeligArbeid(session) {
            log.info("Henter personer fra spedisjon og forbereder arbeidstabell")

            measureTime {
                HikariDataSource(spedisjonConfig).use { spedisjonDataSource ->
                    sessionOf(spedisjonDataSource).use { spedisjonSession ->
                        val limit = 50_000
                        @Language("PostgreSQL")
                        val stmt = "select fnr from melding group by fnr order by fnr limit $limit offset ?"
                        var page = 0
                        do {
                            val offset = limit * page
                            page += 1

                            log.info("[page $page] henter personer")
                            val personer = spedisjonSession.run(queryOf(stmt, offset).map { row ->
                                row.string("fnr").padStart(11, '0')
                            }.asList)

                            log.info("[page $page] inserter ${personer.size} i arbeidstabell")

                            if (personer.isNotEmpty()) {
                                @Language("PostgreSQL")
                                val query = """INSERT INTO arbeidstabell (fnr) VALUES ${personer.joinToString { "(?)" }}"""
                                session.run(queryOf(query, *personer.toTypedArray()).asExecute)
                            }

                            log.info("[page $page] complete")
                        } while (personer.size >= limit)
                    }
                }
            }.also {
                log.info("Brukte ${it.inWholeSeconds} sekunder på å fylle arbeidstabellen")
            }
        }

        @Language("PostgreSQL")
        val hentSpedisjonhendelser = """
            select id,duplikatkontroll,intern_dokument_id
            from melding 
            where fnr = ? and (
                type = 'inntektsmelding'
            );
        """

        @Language("PostgreSQL")
        val hentSpleishendelser = """
            select id,melding_id,data->>'mottattDato' as mottatt_dato, data->>'arkivreferanse' as arkivreferanse
            from melding 
            where fnr = ? and (
                melding_type = 'INNTEKTSMELDING'
            );
        """
        @Language("PostgreSQL")
        val updateSpedisjonStmt = """
            update melding
            set intern_dokument_id = v.intern_dokument_id
            from (values
                %s
            ) as v(id, intern_dokument_id)
            where melding.id = v.id
        """
        @Language("PostgreSQL")
        val updateSpedisjonAsyncStmt = """
            update ekspedering
            set intern_dokument_id = v.ny_intern_dokument_id
            from (values
                %s
            ) as v(ny_intern_dokument_id,gammel_intern_dokument_id)
            where ekspedering.intern_dokument_id = v.gammel_intern_dokument_id;
        """

        HikariDataSource(spedisjonConfig).use { spedisjonDataSource ->
            sessionOf(spedisjonDataSource).use { spedisjonSession ->
                HikariDataSource(spedisjonAsyncConfig).use { spedisjonAsyncDataSource ->
                    sessionOf(spedisjonAsyncDataSource).use { spedisjonAsyncSession ->
                        HikariDataSource(spleisConfig).use { spleisDataSource ->
                            sessionOf(spleisDataSource).use { spleisSession ->
                                utførArbeid(session) { arbeid ->
                                    MDC.putCloseable("arbeidId", arbeid.id.toString()).use {
                                        try {
                                            log.info("henter hendelser for arbeidId=${arbeid.id}")
                                            val spedisjonhendelser = spedisjonSession.run(queryOf(hentSpedisjonhendelser, arbeid.fnr).map { row ->
                                                Spedisjoninntektsmelding(
                                                    id = row.long("id"),
                                                    duplikatkontroll = row.string("duplikatkontroll"),
                                                    internDokumentId = UUID.fromString(row.string("intern_dokument_id")),
                                                )
                                            }.asList)

                                            log.info("Hentet ${spedisjonhendelser.size} hendelser for arbeidId=${arbeid.id}")

                                            val spleishendelser = spleisSession.run(queryOf(hentSpleishendelser, arbeid.fnr.toLong()).map { row ->
                                                Spleisinntektsmelding(
                                                    id = row.long("id"),
                                                    internDokumentId = UUID.fromString(row.string("melding_id")),
                                                    mottattDato = LocalDateTime.parse(row.string("mottatt_dato")),
                                                    arkivreferanse = row.string("arkivreferanse")
                                                )
                                            }.asList)

                                            log.info("Hentet ${spleishendelser.size} hendelser for arbeidId=${arbeid.id}")

                                            val ulikeHendelser = spleishendelser
                                                .map { spleishendelse ->
                                                    val motpart = spedisjonhendelser.firstOrNull { spedisjonhendelse ->
                                                        spedisjonhendelse.duplikatkontroll == spleishendelse.duplikatkontroll
                                                    }
                                                    spleishendelse to motpart
                                                }
                                                .filter { (spleishendelse, spedisjonhendelse) ->
                                                    spleishendelse.internDokumentId != spedisjonhendelse?.internDokumentId
                                                }
                                                .onEach { (spleishendelse, spedisjonhendelse) ->
                                                    when {
                                                        spedisjonhendelse == null -> log.info("fant ikke tilhørende spedisjonhendelse for spleishendelseId=${spleishendelse.id}")
                                                        else -> {
                                                            log.info("fant ulikhet i internDokumentId mellom spedisjonHendelseId=${spedisjonhendelse.id} og spleishendelseId=${spleishendelse.id}")
                                                        }
                                                    }
                                                }

                                            val spedisjonverdier: List<Any> = ulikeHendelser.flatMap { (spleishendelse, spedisjonhendelse) ->
                                                spedisjonhendelse?.let {
                                                    listOf(
                                                        it.id,
                                                        spleishendelse.internDokumentId
                                                    )
                                                } ?: emptyList()
                                            }
                                            if (spedisjonverdier.isNotEmpty()) {
                                                val updateStmt = updateSpedisjonStmt.format(ulikeHendelser.joinToString { "(?, ?)" })
                                                spedisjonSession.run(queryOf(updateStmt, *spedisjonverdier.toTypedArray()).asUpdate)
                                            }

                                            val spedisjonAsyncverdier: List<Any> = ulikeHendelser.flatMap { (spleishendelse, spedisjonhendelse) ->
                                                spedisjonhendelse?.let {
                                                    listOf(
                                                        spleishendelse.internDokumentId,
                                                        spedisjonhendelse.internDokumentId
                                                    )
                                                } ?: emptyList()
                                            }
                                            if (spedisjonAsyncverdier.isNotEmpty()) {
                                                val updateStmt = updateSpedisjonAsyncStmt.format(ulikeHendelser.joinToString { "(?, ?)" })
                                                spedisjonAsyncSession.run(queryOf(updateStmt, *spedisjonAsyncverdier.toTypedArray()).asUpdate)
                                            }
                                        } catch (err: Exception) {
                                            log.error("feil ved migrering: ${err.message}", err)
                                            throw err
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

data class Spedisjoninntektsmelding(
    val id: Long,
    val duplikatkontroll: String,
    val internDokumentId: UUID
)

enum class Hendelsetype {
    NY_SØKNAD,
    NY_SØKNAD_FRILANS,
    NY_SØKNAD_SELVSTENDIG,
    NY_SØKNAD_ARBEIDSLEDIG,
    SENDT_SØKNAD_ARBEIDSGIVER,
    SENDT_SØKNAD_NAV,
    SENDT_SØKNAD_FRILANS,
    SENDT_SØKNAD_SELVSTENDIG,
    SENDT_SØKNAD_ARBEIDSLEDIG;

    companion object {
        fun tilHendelsetypeOrNull(s: String): Hendelsetype? {
            return when (s.lowercase()) {
                "ny_søknad" -> NY_SØKNAD
                "ny_søknad_frilans" -> NY_SØKNAD_FRILANS
                "ny_søknad_selvstendig" -> NY_SØKNAD_SELVSTENDIG
                "ny_søknad_arbeidsledig" -> NY_SØKNAD_ARBEIDSLEDIG
                "sendt_søknad_arbeidsgiver" -> SENDT_SØKNAD_ARBEIDSGIVER
                "sendt_søknad_nav" -> SENDT_SØKNAD_NAV
                "sendt_søknad_frilans" -> SENDT_SØKNAD_FRILANS
                "sendt_søknad_selvstendig" -> SENDT_SØKNAD_SELVSTENDIG
                "sendt_søknad_arbeidsledig" -> SENDT_SØKNAD_ARBEIDSLEDIG
                else -> null
            }
        }
    }
}

internal fun String.sha512(): String =
    MessageDigest
        .getInstance("SHA-512")
        .digest(this.toByteArray())
        .joinToString("") { "%02x".format(it) }

data class Spleisinntektsmelding(
    val id: Long,
    val internDokumentId: UUID,
    val arkivreferanse: String,
    val mottattDato: LocalDateTime
) {
    val duplikatkontroll by lazy {
        "$arkivreferanse".sha512()
    }
}

data class Arbeid(val id: Long, val fnr: String)

private fun utførArbeid(session: Session, arbeider: (arbeid: Arbeid) -> Unit) {
    do {
        log.info("Forsøker å hente arbeid")
        val arbeidsliste = hentArbeid(session)
            .also {
                if (it.isNotEmpty()) log.info("Fikk ${it.size} stk")
            }
            .onEach {
                try {
                    arbeider(it)
                } finally {
                    arbeidFullført(session, it)
                }
            }
    } while (arbeidsliste.isNotEmpty())
    log.info("Fant ikke noe arbeid, avslutter")
}

private fun fåLås(session: Session): Boolean {
    // oppretter en lås som varer ut levetiden til sesjonen.
    // returnerer umiddelbart med true/false avhengig om vi fikk låsen eller ikke
    @Language("PostgreSQL")
    val query = "SELECT pg_try_advisory_lock(1337)"
    return session.run(queryOf(query).map { it.boolean(1) }.asSingle)!!
}

private fun hentArbeid(session: Session, size: Int = 500): List<Arbeid> {
    @Language("PostgreSQL")
    val query = """
    select id,fnr from arbeidstabell where arbeid_startet IS NULL limit $size for update skip locked; 
    """
    @Language("PostgreSQL")
    val oppdater = "update arbeidstabell set arbeid_startet=now() where id IN(%s)"
    return session.transaction { txSession ->
        txSession.run(queryOf(query).map {
            Arbeid(
                id = it.long("id"),
                fnr = it.string("fnr").padStart(11, '0')
            )
        }.asList).also { personer ->
            if (personer.isNotEmpty()) {
                txSession.run(queryOf(String.format(oppdater, personer.joinToString { "?" }), *personer.map { it.id }.toTypedArray()).asUpdate)
            }
        }
    }
}
private fun arbeidFullført(session: Session, arbeid: Arbeid) {
    @Language("PostgreSQL")
    val query = "update arbeidstabell set arbeid_ferdig=now() where id=?"
    session.run(queryOf(query, arbeid.id).asUpdate)
}
private fun arbeidFinnes(session: Session): Boolean {
    @Language("PostgreSQL")
    val query = "SELECT COUNT(1) as antall FROM arbeidstabell"
    val antall = session.run(queryOf(query).map { it.long("antall") }.asSingle) ?: 0
    return antall > 0
}

private fun klargjørEllerVentPåTilgjengeligArbeid(session: Session, fyllArbeidstabell: () -> Unit) {
    if (fåLås(session)) {
        if (arbeidFinnes(session)) return
        return fyllArbeidstabell()
    }

    log.info("Venter på at arbeid skal bli tilgjengelig")
    while (!arbeidFinnes(session)) {
        log.info("Arbeid finnes ikke ennå, venter litt")
        runBlocking { delay(250) }
    }
}

private fun migrateDatabase(connectionConfig: HikariConfig) {
    HikariDataSource(connectionConfig).use { dataSource ->
        val flyway = Flyway.configure()
            .dataSource(dataSource)
            .validateMigrationNaming(true)
            .lockRetryCount(-1)
            .load()
        flyway.migrate()
    }
}

private fun testTilkoblinger(vararg config: HikariConfig) {
    log.info("Tester datasourcer")
    config.forEach {
        log.info("tester ${it.poolName}")
        HikariDataSource(it).use {
            log.info("${it.poolName} OK!")
        }
    }
}

private fun jdbcUrlForPrivateInstance(path: String, certsPath: String): String {
    val dir = Path(path)
    val hostname = dir.resolve("DATABASE_HOST").readText()
    val port = dir.resolve("DATABASE_PORT").readText().toInt()
    val databaseName = dir.resolve("DATABASE_DATABASE").readText()
    val username = dir.resolve("DATABASE_USERNAME").readText()
    val password = dir.resolve("DATABASE_PASSWORD").readText()
    val sslmode = dir.resolve("DATABASE_SSLMODE").readText()

    val options = mapOf(
        "user" to username,
        "password" to password,
        "sslcert" to "$certsPath/cert.pem",
        "sslrootcert" to "$certsPath/root-cert.pem",
        "sslkey" to "$certsPath/key.pk8",
        "sslmode" to sslmode
    )

    val optionsString = optionsString(options)
    return "jdbc:postgresql://$hostname:$port/$databaseName?$optionsString"
}

private fun optionsString(options: Map<String, String>): String {
    return options.entries.joinToString(separator = "&") { (k, v) -> "$k=$v" }
}
