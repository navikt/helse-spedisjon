package no.nav.helse.spedisjon.migrering

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.kv
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.UUID
import javax.sql.DataSource
import kotlin.io.path.Path
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
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
        jdbcUrl = cloudSqlConnectionString(
            gcpProjectId = System.getenv("GCP_PROJECT_ID"),
            databaseInstance = System.getenv("SPEDISJON_MIGRATE_INSTANCE"),
            databaseName = System.getenv("DATABASE_DATABASE"),
            databaseRegion = System.getenv("GCP_SQL_REGION")
        )
        username = System.getenv("DATABASE_USERNAME")
        password = System.getenv("DATABASE_PASSWORD")
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

    val spedisjonConfig = HikariConfig().apply {
        connectionConfigFromMountPath(System.getenv("SPEDISJON_INSTANCE"), "/var/run/secrets/sql/spedisjon")
            .copyStateTo(this)
        poolName = "spedisjon"
        maximumPoolSize = 1
    }
    val spreSubsumsjon = HikariConfig().apply {
        connectionConfigFromMountPath(System.getenv("SPRE_SUBSUMSJON_INSTANCE"), "/var/run/secrets/sql/spre_subsumsjon")
            .copyStateTo(this)
        poolName = "spre-subsumsjon"
        maximumPoolSize = 1
    }
    val spleisConfig = HikariConfig().apply {
        connectionConfigFromMountPath(System.getenv("SPLEIS_INSTANCE"), "/var/run/secrets/sql/spleis")
            .copyStateTo(this)
        poolName = "spleis"
        maximumPoolSize = 1
    }

    testTilkoblinger(flywayMigrationConfig, appConfig, spedisjonConfig, spreSubsumsjon, spleisConfig)
    migrateDatabase(flywayMigrationConfig)

    HikariDataSource(appConfig).use { dataSource ->
        utførMigrering(dataSource, spedisjonConfig, spreSubsumsjon, spleisConfig)
    }
}

fun utførMigrering(dataSource: DataSource, spedisjonConfig: HikariConfig, spreSubsumsjon: HikariConfig, spleisConfig: HikariConfig, sjekkSpleis: Boolean = true) {
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
        val hentHendelser = """
            select id,type,data->>'id' as soknad_id, data->>'sykmeldingId' as sykmelding_id, data->>'inntektsmeldingId' as inntektsmelding_id, data->>'@id' as intern_id
            from melding 
            where fnr = ? and (intern_dokument_id is null or ekstern_dokument_id is null);
        """
        @Language("PostgreSQL")
        val hentDokumentmapping = """
            select hendelse_id,dokument_id,hendelse_type
            from hendelse_dokument_mapping 
            where dokument_id in(%s) OR hendelse_id in(%s);
        """
        @Language("PostgreSQL")
        val hentHendelserFraSpleis = """
            select id,melding_id,melding_type,data->>'id' as soknad_id, data->>'sykmeldingId' as sykmelding_id, data->>'inntektsmeldingId' as inntektsmelding_id 
            from melding
            where fnr = ? and (
                melding_type = 'NY_SØKNAD' OR
                melding_type = 'NY_SØKNAD_FRILANS' OR
                melding_type = 'NY_SØKNAD_SELVSTENDIG' OR
                melding_type = 'NY_SØKNAD_ARBEIDSLEDIG' OR
                melding_type = 'SENDT_SØKNAD_ARBEIDSGIVER' OR
                melding_type = 'SENDT_SØKNAD_NAV' OR
                melding_type = 'SENDT_SØKNAD_FRILANS' OR
                melding_type = 'SENDT_SØKNAD_SELVSTENDIG' OR
                melding_type = 'SENDT_SØKNAD_ARBEIDSLEDIG' OR
                melding_type = 'INNTEKTSMELDING' OR
                melding_type = 'AVBRUTT_SØKNAD'
            );
        """
        @Language("PostgreSQL")
        val oppdaterInternId = """
            update melding
            set intern_dokument_id = v.intern_dokument_id, ekstern_dokument_id = v.ekstern_dokument_id
            from (values
                %s
            ) as v(id, intern_dokument_id, ekstern_dokument_id)
            where melding.id = v.id
        """

        HikariDataSource(spedisjonConfig).use { spedisjonDataSource ->
            sessionOf(spedisjonDataSource).use { spedisjonSession ->
                HikariDataSource(spreSubsumsjon).use { spreSubsumsjonDataSource ->
                    sessionOf(spreSubsumsjonDataSource).use { spreSubsumsjonSession ->
                        HikariDataSource(spleisConfig).use { spleisDataSource ->
                            sessionOf(spleisDataSource).use { spleisSession ->

                                utførArbeid(session) { arbeid ->
                                    MDC.putCloseable("arbeidId", arbeid.id.toString()).use {
                                        log.info("henter hendelser for arbeidId=${arbeid.id}")
                                        val hendelser = spedisjonSession.run(queryOf(hentHendelser, arbeid.fnr).map { row ->
                                            val meldingId = row.long("id")
                                            val type = Meldingtype.fraString(row.string("type").lowercase())
                                            val eksternId = when (type) {
                                                Meldingtype.NY_SØKNAD,
                                                Meldingtype.NY_SØKNAD_FRILANS,
                                                Meldingtype.NY_SØKNAD_SELVSTENDIG,
                                                Meldingtype.NY_SØKNAD_ARBEIDSLEDIG -> row.stringOrNull("sykmelding_id")?.let { UUID.fromString(it) } ?: error("sykmelding_id er null for $meldingId")
                                                Meldingtype.SENDT_SØKNAD_ARBEIDSGIVER,
                                                Meldingtype.SENDT_SØKNAD_NAV,
                                                Meldingtype.SENDT_SØKNAD_FRILANS,
                                                Meldingtype.SENDT_SØKNAD_SELVSTENDIG,
                                                Meldingtype.SENDT_SØKNAD_ARBEIDSLEDIG,
                                                Meldingtype.AVBRUTT_SØKNAD,
                                                Meldingtype.AVBRUTT_ARBEIDSLEDIG_SØKNAD -> row.stringOrNull("soknad_id")?.let { UUID.fromString(it) } ?: error("soknad_id er null for $meldingId")
                                                Meldingtype.INNTEKTSMELDING -> row.stringOrNull("inntektsmelding_id")?.let { UUID.fromString(it) } ?: error("inntektsmelding_id er null for $meldingId")
                                            }
                                            Hendelse(
                                                id = meldingId,
                                                type = type,
                                                eksternId = eksternId,
                                                internId = row.stringOrNull("intern_id")?.let { UUID.fromString(it) } ?: error("intern_id er null for $meldingId")
                                            )
                                        }.asList)

                                        log.info("Hentet ${hendelser.size} hendelser for arbeidId=${arbeid.id}")
                                        if (hendelser.isNotEmpty()) {
                                            val eksterneIder = hendelser.map { it.eksternId }
                                            val interneIder = hendelser.map { it.internId }
                                            val dokumentmappingStmt = hentDokumentmapping.format(eksterneIder.joinToString { "?" }, interneIder.joinToString { "?" })

                                            val dokumentmapping = spreSubsumsjonSession.run(queryOf(dokumentmappingStmt, *eksterneIder.toTypedArray(), *interneIder.toTypedArray()).map { row ->
                                                Dokumentmapping(
                                                    internId = row.string("hendelse_id").let { UUID.fromString(it) },
                                                    eksternId = row.string("dokument_id").let { UUID.fromString(it) },
                                                    hendelsetype = Meldingtype.fraString(row.string("hendelse_type").lowercase())
                                                )
                                            }.asList)

                                            val spleishendelser by lazy {
                                                spleisSession.run(queryOf(hentHendelserFraSpleis, arbeid.fnr.toLong()).map { row ->
                                                    val spleisMeldingId = row.long("id")
                                                    val spleismeldingtype = SpleisMeldingstype.valueOf(row.string("melding_type"))
                                                    val meldingtype = when (spleismeldingtype) {
                                                        SpleisMeldingstype.NY_SØKNAD -> Meldingtype.NY_SØKNAD
                                                        SpleisMeldingstype.NY_SØKNAD_FRILANS -> Meldingtype.NY_SØKNAD_FRILANS
                                                        SpleisMeldingstype.NY_SØKNAD_SELVSTENDIG -> Meldingtype.NY_SØKNAD_SELVSTENDIG
                                                        SpleisMeldingstype.NY_SØKNAD_ARBEIDSLEDIG -> Meldingtype.NY_SØKNAD_ARBEIDSLEDIG
                                                        SpleisMeldingstype.SENDT_SØKNAD_ARBEIDSGIVER -> Meldingtype.SENDT_SØKNAD_ARBEIDSGIVER
                                                        SpleisMeldingstype.SENDT_SØKNAD_NAV -> Meldingtype.SENDT_SØKNAD_NAV
                                                        SpleisMeldingstype.SENDT_SØKNAD_FRILANS -> Meldingtype.SENDT_SØKNAD_FRILANS
                                                        SpleisMeldingstype.SENDT_SØKNAD_SELVSTENDIG -> Meldingtype.SENDT_SØKNAD_SELVSTENDIG
                                                        SpleisMeldingstype.SENDT_SØKNAD_ARBEIDSLEDIG -> Meldingtype.SENDT_SØKNAD_ARBEIDSLEDIG
                                                        SpleisMeldingstype.INNTEKTSMELDING -> Meldingtype.INNTEKTSMELDING
                                                        SpleisMeldingstype.AVBRUTT_SØKNAD -> Meldingtype.AVBRUTT_SØKNAD
                                                    }
                                                    val eksternId = when (meldingtype) {
                                                        Meldingtype.NY_SØKNAD,
                                                        Meldingtype.NY_SØKNAD_FRILANS,
                                                        Meldingtype.NY_SØKNAD_SELVSTENDIG,
                                                        Meldingtype.NY_SØKNAD_ARBEIDSLEDIG -> row.stringOrNull("sykmelding_id")?.let { UUID.fromString(it) } ?: error("sykmelding_id er null for $spleisMeldingId")
                                                        Meldingtype.SENDT_SØKNAD_ARBEIDSGIVER,
                                                        Meldingtype.SENDT_SØKNAD_NAV,
                                                        Meldingtype.SENDT_SØKNAD_FRILANS,
                                                        Meldingtype.SENDT_SØKNAD_SELVSTENDIG,
                                                        Meldingtype.SENDT_SØKNAD_ARBEIDSLEDIG,
                                                        Meldingtype.AVBRUTT_SØKNAD,
                                                        Meldingtype.AVBRUTT_ARBEIDSLEDIG_SØKNAD -> row.stringOrNull("soknad_id")?.let { UUID.fromString(it) } ?: error("soknad_id er null for $spleisMeldingId")
                                                        Meldingtype.INNTEKTSMELDING -> row.stringOrNull("inntektsmelding_id")?.let { UUID.fromString(it) } ?: error("inntektsmelding_id er null for $spleisMeldingId")
                                                    }
                                                    Spleishendelse(
                                                        id = spleisMeldingId,
                                                        internId = row.string("melding_id").let { UUID.fromString(it) },
                                                        meldingtype = meldingtype,
                                                        eksternId = eksternId
                                                    )
                                                }.asList)
                                            }

                                            /**
                                             * Inntektsmelding:
                                             *  hendelse_id = <rapid @id>
                                             *  dokument_id = inntektsmeldingId
                                             *  hendelse_type = inntektsmelding
                                             *  dokument_id_type = Inntektsmelding
                                             *
                                             * Ny søknad:
                                             *  hendelse_id = <rapid @id>
                                             *  dokument_id = sykmeldingId (fra flex-søknaden)
                                             *  hendelse_type = ny_søknad (og andre ny_søknad-varianter)
                                             *  dokument_id_type = Sykmelding
                                             *
                                             * Sendt søknad:
                                             *  hendelse_id = <rapid @id>
                                             *  dokument_id = id (fra flex-søknaden)
                                             *  hendelse_type = sendt_søknad_nav (og andre sendt_søknad-varianter)
                                             *  dokument_id_type = Søknad
                                             *
                                             *  hendelse_id = <rapid @id>
                                             *  dokument_id = sykmeldingId (fra flex-søknaden)
                                             *  hendelse_type = sendt_søknad_nav (og andre sendt_søknad-varianter)
                                             *  dokument_id_type = Sykmelding
                                             */

                                            val oppdaterteIder = hendelser.map { hendelse ->
                                                val matchPåInternId = dokumentmapping.firstOrNull { dokumentmapping ->
                                                    dokumentmapping.internId == hendelse.internId
                                                }

                                                // om vi har treff på intern id så er saken grei
                                                if (matchPåInternId != null) {
                                                    log.info("fant direkte treff på intern id for {} ({})", hendelse.kvmeldingId, hendelse.kvmeldingType)
                                                    hendelse
                                                }
                                                else {
                                                    val matchPåEksternId = dokumentmapping.filter { dokumentmapping ->
                                                        dokumentmapping.eksternId == hendelse.eksternId && dokumentmapping.hendelsetype == hendelse.type
                                                    }

                                                    // om vi ikke finner noen treff på ekstern_dokument_id så må vi bare bruke den vi fant i json under @id
                                                    if (matchPåEksternId.isEmpty()) {
                                                        log.info("fant ingen treff på ekstern id og hendelsetype id for {} ({}). sjekker derfor spleishendelser", hendelse.kvmeldingId, hendelse.kvmeldingType)
                                                        val spleisHendelse = if (sjekkSpleis) spleishendelser.firstOrNull { spleishendelse ->
                                                            spleishendelse.eksternId == hendelse.eksternId && spleishendelse.meldingtype == hendelse.type
                                                        } else null
                                                        if (spleisHendelse != null) {
                                                            log.info("fant treff i spleis på ekstern id og hendelsetype id for {} ({}).", hendelse.kvmeldingId, hendelse.kvmeldingType)
                                                            hendelse.copy(
                                                                internId = spleisHendelse.internId
                                                            )
                                                        } else {
                                                            log.info("fant ingen treff på ekstern id og hendelsetype id for {} ({}) etter å ha sjekket spleishendelser", hendelse.kvmeldingId, hendelse.kvmeldingType)
                                                            hendelse
                                                        }
                                                    }
                                                    else {
                                                        log.info("fant treff på ekstern id og hendelsetype for {} ({})", hendelse.kvmeldingId, hendelse.kvmeldingType)
                                                        // hvis vi finner dokument_id, men annen hendelse, er dette mest sannsynlig ny_søknad.
                                                        // ny_søknad har samme eksterne ID (søkadID) som sendt_søknad,
                                                        // men ny_søknad har ikke nødvendigvis blitt satt inn i spre-subsumsjon
                                                        hendelse.copy(internId = matchPåEksternId.first().internId)
                                                    }
                                                }
                                            }

                                            val verdier: List<Any> = oppdaterteIder.flatMap { hendelse ->
                                                listOf(
                                                    hendelse.id,
                                                    hendelse.internId,
                                                    hendelse.eksternId
                                                )
                                            }
                                            val updateStmt = oppdaterInternId.format(oppdaterteIder.joinToString { "(?, ?, ?)" })
                                            spedisjonSession.run(queryOf(updateStmt, *verdier.toTypedArray()).asUpdate)
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

data class Dokumentmapping(
    val internId: UUID,
    val eksternId: UUID,
    val hendelsetype: Meldingtype
)

data class Spleishendelse(
    val id: Long,
    val internId: UUID,
    val meldingtype: Meldingtype,
    val eksternId: UUID
)

data class Hendelse(
    val id: Long,
    val type: Meldingtype,
    val eksternId: UUID,
    val internId: UUID
) {
    val kvmeldingId = kv("meldingId", id)
    val kvmeldingType = kv("meldingType", type)
}

private enum class SpleisMeldingstype {
    NY_SØKNAD,
    NY_SØKNAD_FRILANS,
    NY_SØKNAD_SELVSTENDIG,
    NY_SØKNAD_ARBEIDSLEDIG,
    SENDT_SØKNAD_ARBEIDSGIVER,
    SENDT_SØKNAD_NAV,
    SENDT_SØKNAD_FRILANS,
    SENDT_SØKNAD_SELVSTENDIG,
    SENDT_SØKNAD_ARBEIDSLEDIG,
    INNTEKTSMELDING,
    AVBRUTT_SØKNAD
}

enum class Meldingtype {
    NY_SØKNAD,
    NY_SØKNAD_FRILANS,
    NY_SØKNAD_SELVSTENDIG,
    NY_SØKNAD_ARBEIDSLEDIG,
    SENDT_SØKNAD_ARBEIDSGIVER,
    SENDT_SØKNAD_NAV,
    SENDT_SØKNAD_FRILANS,
    SENDT_SØKNAD_SELVSTENDIG,
    SENDT_SØKNAD_ARBEIDSLEDIG,
    AVBRUTT_SØKNAD,
    AVBRUTT_ARBEIDSLEDIG_SØKNAD,
    INNTEKTSMELDING;

    companion object {
        fun fraString(type: String) = when (type) {
            "ny_søknad" -> NY_SØKNAD
            "ny_søknad_frilans" -> NY_SØKNAD_FRILANS
            "ny_søknad_selvstendig" -> NY_SØKNAD_SELVSTENDIG
            "ny_søknad_arbeidsledig" -> NY_SØKNAD_ARBEIDSLEDIG
            "sendt_søknad_arbeidsgiver" -> SENDT_SØKNAD_ARBEIDSGIVER
            "sendt_søknad_nav" -> SENDT_SØKNAD_NAV
            "sendt_søknad_frilans" -> SENDT_SØKNAD_FRILANS
            "sendt_søknad_selvstendig" -> SENDT_SØKNAD_SELVSTENDIG
            "sendt_søknad_arbeidsledig" -> SENDT_SØKNAD_ARBEIDSLEDIG
            "avbrutt_søknad" -> AVBRUTT_SØKNAD
            "avbrutt_arbeidsledig_søknad" -> AVBRUTT_ARBEIDSLEDIG_SØKNAD
            "inntektsmelding" -> INNTEKTSMELDING
            else -> error("Ukjent meldingtype: $type")
        }
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
            .cleanDisabled(false)
            .load()
        flyway.migrate()
    }
}

private fun testTilkoblinger(vararg config: HikariConfig) {
    log.info("Tester datasourcer")
    config.forEach {
        HikariDataSource(it).use {
            log.info("${it.poolName} OK!")
        }
    }
}

private fun connectionConfigFromMountPath(instanceId: String, mountPath: String): HikariConfig {
    val secretsPath = Path(mountPath).listDirectoryEntries()
    /* søker etter filer som slutter på det vi forventer, fordi miljøvariabelene/filnavnet kan ha
        en custom prefiks som hver app har bestemt selv
     */

    log.info("fant følgende filer under $mountPath: ${secretsPath.joinToString { it.name } }")

    val databaseName = secretsPath.first { it.name.endsWith("_DATABASE") }.readText()
    val databaseUsername = secretsPath.first { it.name.endsWith("_USERNAME") }.readText()
    val databasePassword = secretsPath.first { it.name.endsWith("_PASSWORD") }.readText()

    return HikariConfig().apply {
        jdbcUrl = cloudSqlConnectionString(
            gcpProjectId = System.getenv("GCP_PROJECT_ID"),
            databaseInstance = instanceId,
            databaseName = databaseName,
            databaseRegion = System.getenv("GCP_SQL_REGION")
        )
        username = databaseUsername
        password = databasePassword
    }
}
private fun cloudSqlConnectionString(gcpProjectId: String, databaseInstance: String, databaseName: String, databaseRegion: String): String {
    return String.format(
        "jdbc:postgresql:///%s?%s&%s",
        databaseName,
        "cloudSqlInstance=$gcpProjectId:$databaseRegion:$databaseInstance",
        "socketFactory=com.google.cloud.sql.postgres.SocketFactory"
    )
}