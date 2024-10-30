package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.speed.HistoriskeIdenterResponse
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.PersonResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.github.navikt.tbd_libs.test_support.TestDataSource
import io.mockk.every
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime

val databaseContainer = DatabaseContainers.container("spedisjon", CleanupStrategy.tables("inntektsmelding,berikelse,melding"))

abstract class AbstractDatabaseTest {
    private lateinit var testDataSource: TestDataSource
    protected val dataSource get() = testDataSource.ds

    protected companion object {
        const val FØDSELSNUMMER = "fnr"
        const val ORGNUMMER = "a1"
        const val AKTØR = "aktørId"
        val OPPRETTET_DATO: LocalDateTime = LocalDateTime.now()
    }

    @BeforeEach
    fun setup() {
        testDataSource = databaseContainer.nyTilkobling()
        testDataSource.ds
    }

    @AfterEach
    fun `stop postgres`() {
        databaseContainer.droppTilkobling(testDataSource)
    }


    protected fun antallMeldinger(fnr: String = FØDSELSNUMMER) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM melding WHERE fnr = ?", fnr).map { row ->
                row.long(1)
            }.asSingle)
        }

    protected fun manipulerTimeoutInntektsmelding(fødselsnummer: String, trekkFra: Duration = Duration.ofSeconds(301)) {
        @Language("PostgreSQL")
        val statement = """UPDATE inntektsmelding SET timeout = (SELECT max(timeout) FROM inntektsmelding WHERE fnr = :fnr) - INTERVAL '${trekkFra.seconds} SECONDS'"""
        using(sessionOf(dataSource)) {session ->
            session.run(queryOf(statement, mapOf("fnr" to fødselsnummer, "trekkFra" to trekkFra)).asUpdate)
        }
    }

    protected fun mockSpeed(fnr: String = FØDSELSNUMMER, aktørId: String = AKTØR, fødselsdato: LocalDate = LocalDate.of(1950, 10, 27), dødsdato: LocalDate? = null, støttes: Boolean = true): SpeedClient {
        return mockk<SpeedClient> {
            every { hentPersoninfo(fnr, any() )} returns PersonResponse(
                fødselsdato = fødselsdato,
                dødsdato = dødsdato,
                fornavn = "TEST",
                mellomnavn = null,
                etternavn = "PERSON",
                adressebeskyttelse = if (støttes) PersonResponse.Adressebeskyttelse.UGRADERT else PersonResponse.Adressebeskyttelse.STRENGT_FORTROLIG,
                kjønn = PersonResponse.Kjønn.MANN
            )
            every { hentHistoriskeFødselsnumre(fnr, any()) } returns HistoriskeIdenterResponse(emptyList())
            every { hentFødselsnummerOgAktørId(fnr, any()) } returns IdentResponse(
                fødselsnummer = fnr,
                aktørId = aktørId,
                npid = null,
                kilde = IdentResponse.KildeResponse.PDL
            )
        }
    }
}