from __future__ import absolute_import, unicode_literals

from django.db import DatabaseError
from django.db.models.expressions import Ref
from django.db.transaction import TransactionManagementError
import django.db.models.sql.compiler
import sqlserver_ado.compiler


if django.VERSION >= (1, 9, 0):
    def _get_where(compiler):
        return compiler.where

    def _get_having(compiler):
        return compiler.having
else:
    def _get_where(compiler):
        return compiler.query.where

    def _get_having(compiler):
        return compiler.query.having


class SQLCompiler(sqlserver_ado.compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Copied from https://github.com/michiya/django-pyodbc-azure

        Creates the SQL for this query. Returns the SQL string and list of
        parameters.
        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        refcounts_before = self.query.alias_refcount.copy()
        try:
            extra_select, order_by, group_by = self.pre_sql_setup()

            # The do_offset flag indicates whether we need to construct
            # the SQL needed to use limit/offset w/SQL Server.
            high_mark = self.query.high_mark
            low_mark = self.query.low_mark
            do_limit = with_limits and high_mark is not None
            do_offset = with_limits and low_mark != 0
            # SQL Server 2012 or newer supports OFFSET/FETCH clause
            supports_offset_clause = True  #self.connection.sql_server_version >= 2012
            do_offset_emulation = do_offset and not supports_offset_clause

            distinct_fields = self.get_distinct()

            # This must come after 'select', 'ordering', and 'distinct' -- see
            # docstring of get_from_clause() for details.
            from_, f_params = self.get_from_clause()

            for_update_part = None
            where, w_params = self.compile(self.where) if self.where is not None else ("", [])
            having, h_params = self.compile(self.having) if self.having is not None else ("", [])

            combinator = self.query.combinator
            features = self.connection.features
            if combinator:
                if not getattr(features, 'supports_select_{}'.format(combinator)):
                    raise DatabaseError('{} not supported on this database backend.'.format(combinator))
                result, params = self.get_combinator_sql(combinator, self.query.combinator_all)
            else:
                params = []
                result = ['SELECT']

                if self.query.distinct:
                    result.append(self.connection.ops.distinct_sql(distinct_fields))

                # SQL Server requires the keword for limitting at the begenning
                if do_limit and not do_offset:
                    result.append('TOP %d' % high_mark)

                out_cols = []
                col_idx = 1
                for _, (s_sql, s_params), alias in self.select + extra_select:
                    if alias:
                        s_sql = '%s AS %s' % (s_sql, self.connection.ops.quote_name(alias))
                    elif with_col_aliases or do_offset_emulation:
                        s_sql = '%s AS %s' % (s_sql, 'Col%d' % col_idx)
                        col_idx += 1
                    params.extend(s_params)
                    out_cols.append(s_sql)

                # SQL Server requires an order-by clause for offsetting
                if do_offset:
                    meta = self.query.get_meta()
                    qn = self.quote_name_unless_alias
                    offsetting_order_by = '%s.%s' % (qn(meta.db_table), qn(meta.pk.db_column or meta.pk.column))
                    if do_offset_emulation:
                        if order_by:
                            ordering = []
                            for expr, (o_sql, o_params, _) in order_by:
                                # value_expression in OVER clause cannot refer to
                                # expressions or aliases in the select list. See:
                                # http://msdn.microsoft.com/en-us/library/ms189461.aspx
                                src = next(iter(expr.get_source_expressions()))
                                if isinstance(src, Ref):
                                    src = next(iter(src.get_source_expressions()))
                                    o_sql, _ = src.as_sql(self, self.connection)
                                    odir = 'DESC' if expr.descending else 'ASC'
                                    o_sql = '%s %s' % (o_sql, odir)
                                ordering.append(o_sql)
                                params.extend(o_params)
                            offsetting_order_by = ', '.join(ordering)
                            order_by = []
                        out_cols.append('ROW_NUMBER() OVER (ORDER BY %s) AS [rn]' % offsetting_order_by)
                    elif not order_by:
                        order_by.append(((None, ('%s ASC' % offsetting_order_by, [], None))))

                result.append(', '.join(out_cols))

                if self.query.select_for_update and self.connection.features.has_select_for_update:
                    if self.connection.get_autocommit():
                        raise TransactionManagementError('select_for_update cannot be used outside of a transaction.')

                    nowait = self.query.select_for_update_nowait
                    skip_locked = self.query.select_for_update_skip_locked
                    # If it's a NOWAIT/SKIP LOCKED query but the backend
                    # doesn't support it, raise a DatabaseError to prevent a
                    # possible deadlock.
                    if nowait and not self.connection.features.has_select_for_update_nowait:
                        raise DatabaseError('NOWAIT is not supported on this database backend.')
                    elif skip_locked and not self.connection.features.has_select_for_update_skip_locked:
                        raise DatabaseError('SKIP LOCKED is not supported on this database backend.')
                    for_update_part = self.connection.ops.for_update_sql(nowait=nowait, skip_locked=skip_locked)

                if for_update_part and self.connection.features.for_update_after_from:
                    from_.insert(1, for_update_part)

                result.append('FROM')
                result.extend(from_)
                params.extend(f_params)

                if where:
                    result.append('WHERE %s' % where)
                    params.extend(w_params)

                grouping = []
                for g_sql, g_params in group_by:
                    grouping.append(g_sql)
                    params.extend(g_params)
                if grouping:
                    if distinct_fields:
                        raise NotImplementedError(
                            "annotate() + distinct(fields) is not implemented.")
                    if not order_by:
                        order_by = self.connection.ops.force_no_ordering()
                    result.append('GROUP BY %s' % ', '.join(grouping))

                if having:
                    result.append('HAVING %s' % having)
                    params.extend(h_params)

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                result.append('ORDER BY %s' % ', '.join(ordering))

            # SQL Server requires the backend-specific emulation (2008 or earlier)
            # or an offset clause (2012 or newer) for offsetting
            if do_offset:
                if do_offset_emulation:
                    # Construct the final SQL clause, using the initial select SQL
                    # obtained above.
                    result = ['SELECT * FROM (%s) AS X WHERE X.rn' % ' '.join(result)]
                    # Place WHERE condition on `rn` for the desired range.
                    if do_limit:
                        result.append('BETWEEN %d AND %d' % (low_mark + 1, high_mark))
                    else:
                        result.append('>= %d' % (low_mark + 1))
                    if not self.query.subquery:
                        result.append('ORDER BY X.rn')
                else:
                    result.append('OFFSET %d ROWS' % low_mark)
                    if do_limit:
                        result.append('FETCH FIRST %d ROWS ONLY' % (high_mark - low_mark))

            return ' '.join(result), tuple(params)
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)


class SQLInsertCompiler(sqlserver_ado.compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(sqlserver_ado.compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(sqlserver_ado.compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(sqlserver_ado.compiler.SQLAggregateCompiler, SQLCompiler):
    pass


try:
    class SQLDateCompiler(sqlserver_ado.compiler.SQLDateCompiler, SQLCompiler):
        pass
except AttributeError:
    pass


try:
    class SQLDateTimeCompiler(sqlserver_ado.compiler.SQLDateTimeCompiler, SQLCompiler):
        pass
except AttributeError:
    pass
