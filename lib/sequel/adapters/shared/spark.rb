# frozen-string-literal: true

require "sequel/adapters/utils/unmodified_identifiers"

module Sequel
  module Spark
    Sequel::Database.set_shared_adapter_scheme(:spark, self)

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      def create_schema(schema_name, opts = OPTS)
        run(create_schema_sql(schema_name, opts))
      end

      def database_type
        :spark
      end

      def drop_schema(schema_name, opts = OPTS)
        run(drop_schema_sql(schema_name, opts))
      end

      # Spark does not support primary keys, so do not
      # add any options
      def serial_primary_key_options
        # We could raise an exception here instead of just
        # ignoring the primary key setting.
        { type: Integer }
      end

      def supports_create_table_if_not_exists?
        true
      end

      def tables(opts = OPTS)
        _mangle_tables(_tables("TABLES", :tableName, opts) - _views(opts), opts)
      end

      # Spark does not support transactions.
      def transaction(_opts = nil)
        yield
      end

      # Use an inline VALUES table.
      def values(v)
        @default_dataset.clone(values: v)
      end

      def views(opts = OPTS)
        _mangle_tables(_views(opts), opts)
      end

      private

      def _tables(type, column, opts)
        sql = String.new
        sql << "SHOW " << type
        if (schema = opts[:schema])
          sql << " IN " << literal(schema)
        end
        if (like = opts[:like])
          sql << " LIKE " << literal(like)
        end

        ds = dataset.with_sql(sql)

        # Always internally qualify, so that if a table name in a schema
        # has the same name as a temporary view, it will not exclude
        # the table name.
        ds.map([ :namespace, column ]).map do |ns, name|
          if ns && !ns.empty?
            Sequel::SQL::QualifiedIdentifier.new(ns, name)
          else
            name.to_sym
          end
        end
      end

      def _views(opts)
        _tables("VIEWS", :viewName, opts)
      end

      def _mangle_tables(tables, opts)
        if opts[:qualify]
          tables
        else
          tables.map { |t| t.is_a?(Sequel::SQL::QualifiedIdentifier) ? t.column.to_sym : t }
        end
      end

      def create_schema_sql(schema_name, opts)
        sql = String.new
        sql << "CREATE SCHEMA "
        sql << "IF NOT EXISTS " if opts[:if_not_exists]
        sql << literal(schema_name)

        if (comment = opts[:comment])
          sql << " COMMENT "
          sql << literal(comment)
        end

        if (location = opts[:location])
          sql << " LOCATION "
          sql << literal(location)
        end

        if (properties = opts[:properties])
          sql << " WITH DBPROPERTIES ("
          properties.each do |k, v|
            sql << literal(k.to_s) << "=" << literal(v.to_s)
          end
          sql << ")"
        end

        sql
      end

      def create_table_prefix_sql(name, options)
        sql = String.new
        sql << "CREATE "
        sql << "EXTERNAL " if options[:external]
        sql << "TEMPORARY " if options[:temp]
        sql << "TABLE "
        sql << "IF NOT EXISTS " if options[:if_not_exists]
        sql << quote_schema_table(name)
        sql
      end

      def create_table_sql(name, generator, options)
        if options[:like] || (options[:using] && generator.columns.empty?)
          _append_table_view_options_sql(create_table_prefix_sql(name, options), options)
        else
          _append_table_view_options_sql(super, options)
        end
      end

      def create_table_as_sql(name, sql, options)
        _append_table_view_options_sql(create_table_prefix_sql(name, options), options) << " AS #{sql}"
      end

      def create_view_sql(name, source, options)
        if source.is_a?(Hash)
          options = source
          source = nil
        end

        sql = String.new
        sql << create_view_sql_append_columns("CREATE #{'OR REPLACE ' if options[:replace]}#{'TEMPORARY ' if options[:temp]}VIEW#{' IF NOT EXISTS' if options[:if_not_exists]} #{quote_schema_table(name)}", options[:columns])

        if source
          source = source.sql if source.is_a?(Dataset)
          sql << " AS " << source
        end

        _append_table_view_options_sql(sql, options)
      end

      def _append_table_view_options_sql(sql, options)
        if (like = options[:like])
          sql << " LIKE " << literal(like)
        end

        if options[:using]
          sql << " USING " << options[:using].to_s
        end

        if (location = options[:location])
          sql << " LOCATION " << literal(location)
        end

        if options[:partitioned_by]
          sql << " PARTITIONED BY "
          _append_column_list_sql(sql, options[:partitioned_by])
        end

        if options[:clustered_by]
          sql << " CLUSTERED BY "
          _append_column_list_sql(sql, options[:clustered_by])

          if options[:sorted_by]
            sql << " SORTED BY "
            _append_column_list_sql(sql, options[:sorted_by])
          end
          raise "Must specify :num_buckets when :clustered_by is used" unless options[:num_buckets]
          sql << " INTO " << literal(options[:num_buckets]) << " BUCKETS"
        end

        if options[:options]
          sql << " OPTIONS ("
          options[:options].each do |k, v|
            sql << literal(k.to_s) << "=" << literal(v.to_s)
          end
          sql << ")"
        end

        sql
      end

      def _append_column_list_sql(sql, columns)
        sql << "("
        schema_utility_dataset.send(:identifier_list_append, sql, Array(columns))
        sql << ")"
      end

      def drop_schema_sql(schema_name, opts)
        sql = String.new
        sql << "DROP SCHEMA "
        sql << "IF EXISTS " if opts[:if_exists]
        sql << literal(schema_name)
        sql << " CASCADE" if opts[:cascade]
        sql
      end

      # Spark SQL does not support CASCADE on DROP TABLE.
      # Support :purge option, ignore :cascade.
      def drop_table_sql(name, options)
        "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_schema_table(name)}#{' PURGE' if options[:purge]}"
      end

      def schema_parse_table(table, opts)
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        metadata_dataset.with_sql("DESCRIBE #{"#{im.call(opts[:schema])}." if opts[:schema]}#{im.call(table)}").map do |row|
          [ m.call(row[:col_name]), { db_type: row[:data_type], type: schema_column_type(row[:data_type]) } ]
        end
      end

      def supports_create_or_replace_view?
        true
      end

      def type_literal_generic_file(_column)
        "binary"
      end

      def type_literal_generic_float(_column)
        "float"
      end

      def type_literal_generic_string(_column)
        "string"
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      Dataset.def_sql_method(self, :select, [ [ "if opts[:values]", %w[values] ], [ "else", %w[with select distinct columns from join where group having compounds order limit] ] ])

      # Handle the date_arithmetic extension's DateAdd expressions using
      # Spark's make_ym_interval/make_dt_interval functions.
      #
      # The extension lets a caller name the result type with the :cast
      # option (DateAdd#cast_type), defaulting to the generic timestamp type,
      # and the result is always cast to it here.
      #
      # The cast is unconditional because Spark's interval arithmetic does
      # not settle on one result type -- it depends on both the input type
      # and which interval function is used. Measured against a live server
      # on the 3.5.0 that ci.yml pins: DATE + make_dt_interval and TIMESTAMP
      # + either function yield a TIMESTAMP, but DATE + make_ym_interval
      # yields a DATE. So there is no subset of cases in which the cast can
      # be skipped and still be honoured, and an interval of all zeroes adds
      # nothing to cast in the first place.
      #
      # Spark's CAST is narrower than other databases': types it does not
      # have (:timestamptz) or cannot convert to (:interval) are rejected by
      # the server with UNSUPPORTED_DATATYPE or DATATYPE_MISMATCH, raising
      # Sequel::DatabaseError. That is deliberately not pre-screened here --
      # an allowlist of castable types in the adapter would go stale against
      # the server, and the server's own message names the offending type.
      def date_add_sql_append(sql, da)
        expr = da.expr

        h = Hash.new(0)
        da.interval.each do |k, v|
          h[k] = v || 0
        end

        h[:days] += h[:weeks] * 7

        if h[:years] != 0 || h[:months] != 0
          expr = Sequel.+(expr, Sequel.function(:make_ym_interval, h[:years], h[:months]))
        end

        if h[:days] != 0 || h[:hours] != 0 || h[:minutes] != 0 || h[:seconds] != 0
          expr = Sequel.+(expr, Sequel.function(:make_dt_interval, h[:days], h[:hours], h[:minutes], h[:seconds]))
        end

        literal_append(sql, Sequel.cast(expr, da.cast_type || Time))
      end

      # Route prepared statement / bound variable deletes through the
      # emulated delete path, since Spark does not support native DELETE.
      def call(type, bind_variables = OPTS, *values, &)
        if type == :delete
          ps = to_prepared_statement(type, values, extend: send(:bound_variable_modules))
          ps.bind(bind_variables).delete
        else
          super
        end
      end

      # Emulate delete by selecting all rows except the ones being deleted
      # into a new table, then overwriting the current table with that new
      # table's contents.
      #
      # This is designed to minimize the changes to the tests, and is
      # not recommended for production use. It is not atomic and it has a
      # residual failure window even after sequel-hexspace-rho hardened it --
      # see _with_temp_table and the README section on emulated DELETE/UPDATE.
      def delete
        _with_temp_table
      end

      def update(columns)
        updated_cols = columns.keys
        other_cols = db.from(first_source_table).columns - updated_cols
        updated_vals = columns.values

        _with_temp_table do |tmp_name|
          db.from(tmp_name).insert([ *updated_cols, *other_cols ], select(*updated_vals, *other_cols))
        end
      end

      # Build `<table>__sequel_delete_emulate` -- in the target's own schema,
      # see _temp_table_name -- holding the rows the operation should leave
      # behind, let the caller adjust it, then INSERT OVERWRITE those rows back
      # into the original table.
      #
      # The original table object is never dropped. That is deliberate: until
      # 2026-10-02 this method dropped the original and renamed the temp table
      # into its place, so a process that died in between left the named table
      # GONE, with its rows sitting under a runtime-generated name that appears
      # nowhere in the caller's source. See sequel-hexspace-rho.
      #
      # WHAT THIS STILL DOES NOT GUARANTEE -- read this before relying on it.
      # INSERT OVERWRITE is unambiguously atomic only on an ACID table. This
      # adapter emits USING <format> only when a caller passes :using, and the
      # create_table below passes none, so the temp table -- and typically the
      # caller's own table -- is whatever the metastore defaults to. Measured
      # against this gem's test server on 2026-10-02: Provider `hive`,
      # TextInputFormat, LazySimpleSerDe. Not ACID.
      #
      # It nonetheless behaved better than "not ACID" implies, and the
      # measurements are recorded here because they bound the risk
      # that is left:
      #
      #   * A job failure part way through the write does NOT truncate the
      #     target. raise_error fired after 150k of 200k rows and the table was
      #     left holding its FULL pre-operation contents, so the old files are
      #     not removed up front -- the write stages and then commits.
      #   * SIGKILLing the client at seven points across the statement never
      #     left the table absent, empty or partial. The thriftserver carries
      #     the statement on server-side after the client vanishes, so the table
      #     held either its pre- or its post-operation rows every time. The same
      #     harness against the old drop/rename sequence destroyed the table on
      #     the first try (400,000 rows stranded under the temp name), so it was
      #     landing in the window.
      #
      # What is left is the commit itself: replacing a multi-file Hive table in
      # place is not one atomic filesystem operation, so a death of the SERVER
      # -- not the client -- inside that commit, or a storage failure there, can
      # still leave the table PRESENT but empty or partial, which a plain SELECT
      # reports silently rather than loudly. That window was NOT reproduced: it
      # needs the thriftserver killed, and this gem's test server is shared.
      # Treat it as unquantified, not as absent.
      #
      # The trade is therefore deliberate and it is not free. A silently wrong
      # read is in some ways worse than a loud table-not-found. What buys it is
      # recoverability: an absent table is unrecoverable by anyone reading their
      # own source, because the name holding the data appears nowhere in it,
      # whereas the rows to keep are recoverable here at every instant -- see
      # the ensure below. The same residual window is written down in the
      # README, because a consumer cannot see this comment.
      #
      # The temp table is dropped in exactly two states, both of which make it a
      # discardable copy:
      #
      #   * the overwrite never started, so the original still holds its
      #     pre-operation contents; or
      #   * the overwrite finished and its row count was verified against the
      #     temp table, so the original holds the intended contents.
      #
      # In any other state -- a raise inside the overwrite, or a row count that
      # disagrees with the temp table -- the temp table holds the only intact
      # copy of the rows to keep and is left alone. That is sequel-hexspace-c5q's
      # rule, carried over from the drop/rename sequence it was written for.
      #
      # Both the leading drop and the ensure exist because this sequence is not
      # atomic and Spark's CREATE TABLE is not idempotent, so a process that
      # dies part way through strands the temp table and every later delete or
      # update on the same table then fails with TABLE_OR_VIEW_ALREADY_EXISTS --
      # an error that names a table nothing in the caller's code refers to.
      # This gem's own suite was poisoned that way on 2026-10-01: one aborted
      # run left `a__sequel_delete_emulate` behind and the next run reported 51
      # errors in tests that never touch table `a`.
      #
      # The ensure handles a raise or an interrupt, which still unwinds; the
      # leading drop handles the rest (SIGKILL, a lost connection, a crash),
      # where nothing of ours gets to run at all. Each names exactly the one
      # table this method creates and owns -- neither enumerates the server,
      # which may be shared with tables belonging to nobody here.
      private def _with_temp_table
        n = count
        table_name = first_source_table
        tmp_name = _temp_table_name(table_name)
        db.drop_table?(tmp_name)
        db.create_table(tmp_name, as: select_all.invert)
        overwrite_started = false
        overwrite_verified = false
        begin
          yield tmp_name if defined?(yield)

          # Counted before the overwrite rather than after, so that what the
          # overwrite is checked against is the intended content itself and not
          # a number derived from the same statement being verified.
          expected = db.from(tmp_name).count

          overwrite_started = true
          _overwrite_from_temp_table(table_name, tmp_name)

          actual = db.from(table_name).count
          unless actual == expected
            raise Sequel::Error, "emulated delete/update of #{literal(table_name)} left #{actual} rows, " \
                                 "expected #{expected}; the rows to keep remain in #{literal(tmp_name)}, " \
                                 "which has deliberately not been dropped"
          end

          overwrite_verified = true
        ensure
          db.drop_table?(tmp_name) if overwrite_verified || !overwrite_started
        end
        n
      end

      # The recovery table's name: the target's own name with
      # `__sequel_delete_emulate` appended, IN THE TARGET'S OWN SCHEMA. The
      # suffix goes on the LAST component only, so any qualifier -- a schema,
      # or a catalog and a schema -- survives intact.
      #
      # Until 2026-10-02 this was `literal(table_name).gsub("`", "") +
      # "__sequel_delete_emulate"`, which stripped the backticks that were
      # keeping a qualified name's parts apart: `sch`.`items` collapsed to the
      # single String "sch.items__sequel_delete_emulate". Sequel does not split
      # a String on the dot -- Dataset#schema_and_table returns [nil, the whole
      # string] for one -- so that went to the server as ONE identifier
      # containing a dot, naming the default database rather than `sch`.
      #
      # That did not merely misplace the recovery copy, it broke the operation
      # outright, which is worth recording because the bug report assumed
      # otherwise. Spark refuses the name at CREATE TABLE:
      #
      #   `sch.items__sequel_delete_emulate` is not a valid name for
      #   tables/databases. Valid names only contain alphabet characters,
      #   numbers and _.
      #
      # Measured against this gem's test server on 2026-10-02. So emulated
      # DELETE and UPDATE against a schema-qualified dataset never worked at
      # all, and no such temp table can ever have existed anywhere for a
      # consumer to depend on. Appending to the last component is therefore a
      # strict widening of the `<table>__sequel_delete_emulate` convention the
      # README documents, not a change to it -- the unqualified case below is
      # byte-for-byte what it was.
      #
      # A UNIQUE per-operation suffix was considered and rejected, even though
      # it would close the concurrent-operation collision the README documents
      # as a hazard. It would put the rows to keep under a runtime-generated
      # name that appears nowhere in the caller's source, and that is the exact
      # property sequel-hexspace-rho removed from this method. It is also
      # unrecoverable in the cases recovery exists for: a SIGKILL or a lost
      # connection raises nothing, so there is no error message for a generated
      # name to be reported in, and the leading drop_table? above cannot find
      # it on the next run either. A predictable name is what makes the README's
      # "look there first" instruction executable. Concurrency stays the
      # caller's problem, documented rather than solved.
      private def _temp_table_name(table_name)
        case table_name
        when SQL::QualifiedIdentifier
          SQL::QualifiedIdentifier.new(table_name.table, "#{table_name.column}__sequel_delete_emulate")
        else
          literal(table_name).gsub("`", "") + "__sequel_delete_emulate"
        end
      end

      # Replace the whole contents of +table_name+ with the rows in +tmp_name+,
      # keeping the original table object. The column list is taken from the
      # TARGET, because INSERT OVERWRITE matches columns by position: naming them
      # explicitly means a column order difference between the two tables would
      # be a visible error rather than silently transposed data.
      #
      # Extracted so a test can stand in for it, which is the only way to pin
      # the "temp table survives an unfinished overwrite" rule without killing a
      # process in the middle of a statement.
      private def _overwrite_from_temp_table(table_name, tmp_name)
        rows = db.from(tmp_name).select(*db.from(table_name).columns)
        db.run("INSERT OVERWRITE TABLE #{literal(table_name)} #{rows.sql}")
      end

      protected def compound_clone(type, dataset, opts)
        dataset = dataset.from_self if dataset.opts[:with]
        super
      end

      def complex_expression_sql_append(sql, op, args)
        case op
        when :<<
          literal_append(sql, Sequel.function(:shiftleft, *args))
        when :>>
          literal_append(sql, Sequel.function(:shiftright, *args))
        when :~
          literal_append(sql, Sequel.function(:regexp, *args))
        when :'!~'
          literal_append(sql, ~Sequel.function(:regexp, *args))
        when :'~*'
          literal_append(sql, Sequel.function(:regexp, Sequel.function(:lower, args[0]), Sequel.function(:lower, args[1])))
        when :'!~*'
          literal_append(sql, ~Sequel.function(:regexp, Sequel.function(:lower, args[0]), Sequel.function(:lower, args[1])))
        else
          super
        end
      end

      def multi_insert_sql_strategy
        :values
      end

      def quoted_identifier_append(sql, name)
        sql << "`" << name.to_s.gsub("`", "``") << "`"
      end

      def requires_sql_standard_datetimes?
        true
      end

      def insert_supports_empty_values?
        false
      end

      def literal_blob_append(sql, v)
        sql << "to_binary('" << [ v ].pack("m*").gsub("\n", "") << "', 'base64')"
      end

      # Spark requires DATE 'YYYY-MM-DD' syntax instead of plain quoted strings
      def literal_date_append(sql, v)
        sql << "DATE "
        super
      end

      # Spark requires TIMESTAMP 'YYYY-MM-DD HH:MM:SS' syntax instead of plain quoted strings
      def literal_time_append(sql, v)
        sql << "TIMESTAMP "
        super
      end

      # Spark requires TIMESTAMP 'YYYY-MM-DD HH:MM:SS' syntax instead of plain quoted strings
      def literal_datetime_append(sql, v)
        sql << "TIMESTAMP "
        super
      end

      def literal_false
        "false"
      end

      def literal_string_append(sql, v)
        sql << "'" << v.gsub(/(['\\])/, '\\\\\1') << "'"
      end

      def literal_true
        "true"
      end

      def supports_cte?(type = :select)
        type == :select
      end

      def supports_cte_in_subqueries?
        true
      end

      def supports_group_cube?
        true
      end

      def supports_group_rollup?
        true
      end

      def supports_grouping_sets?
        true
      end

      def supports_regexp?
        true
      end

      def supports_window_functions?
        true
      end

      # Handle forward references in existing CTEs in the dataset by inserting this
      # dataset before any dataset that would reference it.
      def with(name, dataset, opts = OPTS)
        opts = Hash[opts].merge!(name: name, dataset: dataset).freeze
        references = ReferenceExtractor.references(dataset)

        if (with = @opts[:with])
          with = with.dup
          existing_references = @opts[:with_references]

          if (referencing_dataset = existing_references[literal(name)])
            unless (i = with.find_index { |o| o[:dataset].equal?(referencing_dataset) })
              raise Sequel::Error, "internal error finding referencing dataset"
            end

            with.insert(i, opts)

            # When not inserting dataset at the end, if both the new dataset and the
            # dataset right after it refer to the same reference, keep the reference
            # to the new dataset, so that that dataset is inserted before the new dataset
            # dataset
            existing_references = existing_references.reject do |k, v|
              references[k] && v.equal?(referencing_dataset)
            end
          else
            with << opts
          end

          # Assume we will insert the dataset at the end, so existing references have priority
          references = references.merge(existing_references)
        else
          with = [ opts ]
        end

        clone(with: with.freeze, with_references: references.freeze)
      end

      private def select_values_sql(sql)
        sql << "VALUES "
        expression_list_append(sql, opts[:values])
      end
    end

    # ReferenceExtractor extracts references from datasets that will be used as CTEs.
    class ReferenceExtractor < ASTTransformer
      TABLE_IDENTIFIER_KEYS = [ :from, :join ].freeze
      COLUMN_IDENTIFIER_KEYS = [ :select, :where, :having, :order, :group, :compounds ].freeze

      # Returns a hash of literal string identifier keys referenced by the given
      # dataset with the given dataset as the value for each key.
      def self.references(dataset)
        new(dataset).tap { |ext| ext.transform(dataset) }.references
      end

      attr_reader :references

      def initialize(dataset)
        # `super()` with explicit empty parens, NOT bare `super`. ASTTransformer
        # defines no #initialize, so the inherited one is Object's, which takes
        # no arguments -- bare `super` would forward `dataset` to it and raise
        # ArgumentError on every instantiation. The call is a no-op today and is
        # here so the chain still works if ASTTransformer ever gains state.
        super()
        @dataset = dataset
        @references = {}
      end

      private

      # Extract references from FROM/JOIN, where bare identifiers represent tables.
      def table_identifier_extract(o)
        case o
        when String
          @references[@dataset.literal(Sequel.identifier(o))] = @dataset
        when Symbol, SQL::Identifier
          @references[@dataset.literal(o)] = @dataset
        when SQL::AliasedExpression
          table_identifier_extract(o.expression)
        when SQL::JoinOnClause
          table_identifier_extract(o.table_expr)
          v(o.on)
        when SQL::JoinClause
          table_identifier_extract(o.table_expr)
        else
          v(o)
        end
      end

      # Extract references from datasets, where bare identifiers in most case represent columns,
      # and only qualified identifiers include a table reference.
      def v(o)
        case o
        when Sequel::Dataset
          # Special case FROM/JOIN, because identifiers inside refer to tables and not columns
          TABLE_IDENTIFIER_KEYS.each { |k| o.opts[k]&.each { |jc| table_identifier_extract(jc) } }

          # Look in other keys that may have qualified references or subqueries
          COLUMN_IDENTIFIER_KEYS.each { |k| v(o.opts[k]) }
        when SQL::QualifiedIdentifier
          # If a qualified identifier has a qualified identifier as a key,
          # such as schema.table.column, ignore it, because CTE identifiers shouldn't
          # be schema qualified.
          unless o.table.is_a?(SQL::QualifiedIdentifier)
            @references[@dataset.literal(Sequel.identifier(o.table))] = @dataset
          end
        else
          super
        end
      end
    end
    private_constant :ReferenceExtractor
  end
end
