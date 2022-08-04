package no.nav.helse.spedisjon

import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import javax.sql.DataSource

abstract class AbstractDao(private val dataSource: DataSource) {
    protected fun String.update(argMap: Map<String, Any?>) =
        sessionOf(dataSource).use { it.run(queryOf(this, argMap).asUpdate) }

    protected fun <A> String.listQuery(argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        sessionOf(dataSource).use { it.run(queryOf(this, argMap).map { row -> resultMapping(row) }.asList) }

    protected fun <A> String.singleQuery(argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        sessionOf(dataSource).use { it.run(queryOf(this, argMap).map { row -> resultMapping(row) }.asSingle) }
}