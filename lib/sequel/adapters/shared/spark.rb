# frozen-string-literal: true

require 'sequel/adapters/utils/unmodified_identifiers'

module Sequel
  module Spark
    Sequel::Database.set_shared_adapter_scheme(:spark, self)

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      def create_schema(schema_name, opts=OPTS)
        run(create_schema_sql(schema_name, opts))
      end

      def database_type
        :spark
      end

      def drop_schema(schema_name, opts=OPTS)
        run(drop_schema_sql(schema_name, opts))
      end

      # Spark does not support primary keys, so do not
      # add any options
      def serial_primary_key_options
        # We could raise an exception here instead of just
        # ignoring the primary key setting.
        {:type=>Integer}
      end
 
      def supports_create_table_if_not_exists?
        true
      end

      def tables(opts=OPTS)
        _mangle_tables(_tables("TABLES", :tableName, opts) - _views(opts), opts)
      end

      # Spark does not support transactions.
      def transaction(opts=nil)
        yield
      end

      # Use an inline VALUES table.
      def values(v)
        @default_dataset.clone(:values=>v)
      end

      def views(opts=OPTS)
        _mangle_tables(_views(opts), opts)
      end

      private

      def _tables(type, column, opts)
        sql = String.new
        sql << "SHOW " << type
        if schema = opts[:schema]
          sql << " IN " << literal(schema)
        end
        if like = opts[:like]
          sql << " LIKE " << literal(like)
        end

        ds = dataset.with_sql(sql)

        # Always internally qualify, so that if a table name in a schema
        # has the same name as a temporary view, it will not exclude
        # the table name.
        ds.map([:namespace, column]).map do |ns, name|
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
          tables.map{|t| t.is_a?(Sequel::SQL::QualifiedIdentifier) ? t.column.to_sym : t}
        end
      end

      def create_schema_sql(schema_name, opts)
        sql = String.new
        sql << 'CREATE SCHEMA '
        sql << 'IF NOT EXISTS ' if opts[:if_not_exists]
        sql << literal(schema_name)

        if comment = opts[:comment]
          sql << ' COMMENT '
          sql << literal(comment)
        end

        if location = opts[:location]
          sql << ' LOCATION '
          sql << literal(location)
        end

        if properties = opts[:properties]
          sql << ' WITH DBPROPERTIES ('
          properties.each do |k, v|
            sql << literal(k.to_s) << "=" << literal(v.to_s)
          end
          sql << ')'
        end

        sql
      end

      def create_table_sql(name, generator, options)
        _append_table_view_options_sql(super, options)
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
        sql << create_view_sql_append_columns("CREATE #{'OR REPLACE 'if options[:replace]}#{'TEMPORARY ' if options[:temp]}VIEW#{' IF NOT EXISTS' if options[:if_not_exists]} #{quote_schema_table(name)}", options[:columns])

        if source
          source = source.sql if source.is_a?(Dataset)
          sql << " AS " << source
        end

        _append_table_view_options_sql(sql, options)
      end

      def _append_table_view_options_sql(sql, options)
        if options[:using]
          sql << " USING " << options[:using].to_s
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
          sql << ' OPTIONS ('
          options[:options].each do |k, v|
            sql << literal(k.to_s) << "=" << literal(v.to_s)
          end
          sql << ')'
        end

        sql
      end

      def _append_column_list_sql(sql, columns)
        sql << '(' 
        schema_utility_dataset.send(:identifier_list_append, sql, Array(columns))
        sql << ')'
      end

      def drop_schema_sql(schema_name, opts)
        sql = String.new
        sql << 'DROP SCHEMA '
        sql << 'IF EXISTS ' if opts[:if_exists]
        sql << literal(schema_name)
        sql << ' CASCADE' if opts[:cascade]
        sql
      end

      def schema_parse_table(table, opts)
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        metadata_dataset.with_sql("DESCRIBE #{"#{im.call(opts[:schema])}." if opts[:schema]}#{im.call(table)}").map do |row|
          [m.call(row[:col_name]), {:db_type=>row[:data_type], :type=>schema_column_type(row[:data_type])}]
        end
      end

      def supports_create_or_replace_view?
        true
      end

      def type_literal_generic_file(column)
        'binary'
      end

      def type_literal_generic_float(column)
        'float'
      end

      def type_literal_generic_string(column)
        'string'
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      Dataset.def_sql_method(self, :select, [['if opts[:values]', %w'values'], ['else', %w'with select distinct columns from join where group having compounds order limit']])

      def date_add_sql_append(sql, da)
        expr = da.expr
        cast_type = da.cast_type || Time

        h = Hash.new(0)
        da.interval.each do |k, v|
          h[k] = v || 0
        end

        if h[:weeks]
          h[:days] += h[:weeks] * 7
        end

        if h[:years] != 0 || h[:months] != 0
          expr = Sequel.+(expr, Sequel.function(:make_ym_interval, h[:years], h[:months]))
        end

        if h[:days] != 0 || h[:hours] != 0 || h[:minutes] != 0 || h[:seconds] != 0
          expr = Sequel.+(expr, Sequel.function(:make_dt_interval, h[:days], h[:hours], h[:minutes], h[:seconds]))
        end

        literal_append(sql, expr)
      end

      # Emulate delete by selecting all rows except the ones being deleted
      # into a new table, drop the current table, and rename the new
      # table to the current table name.
      #
      # This is designed to minimize the changes to the tests, and is
      # not recommended for production use.
      def delete
        _with_temp_table
      end

      def update(columns)
        updated_cols = columns.keys
        other_cols = db.from(first_source_table).columns - updated_cols
        updated_vals = columns.values

        _with_temp_table do |tmp_name|
          db.from(tmp_name).insert([*updated_cols, *other_cols], select(*updated_vals, *other_cols))
        end
      end

      private def _with_temp_table
        n = count
        table_name = first_source_table
        tmp_name = literal(table_name).gsub('`', '') + "__sequel_delete_emulate"
        db.create_table(tmp_name, :as=>select_all.invert)
        yield tmp_name if defined?(yield)
        db.drop_table(table_name)
        db.rename_table(tmp_name, table_name)
        n
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
        sql << '`' << name.to_s.gsub('`', '``') << '`'
      end

      def requires_sql_standard_datetimes?
        true
      end

      def insert_supports_empty_values? 
        false
      end

      def literal_blob_append(sql, v)
        sql << "to_binary('" << [v].pack("m*").gsub("\n", "") << "', 'base64')"
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

      def supports_cte?(type=:select)
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
      def with(name, dataset, opts=OPTS)
        opts = Hash[opts].merge!(:name=>name, :dataset=>dataset).freeze
        references = ReferenceExtractor.references(dataset)

        if with = @opts[:with]
          with = with.dup
          existing_references = @opts[:with_references]

          if referencing_dataset = existing_references[literal(name)]
            unless i = with.find_index{|o| o[:dataset].equal?(referencing_dataset)}
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
          with = [opts]
        end

        clone(:with=>with.freeze, :with_references=>references.freeze)
      end

      private def select_values_sql(sql)
        sql << 'VALUES '
        expression_list_append(sql, opts[:values])
      end
    end

    # ReferenceExtractor extracts references from datasets that will be used as CTEs.
    class ReferenceExtractor < ASTTransformer
      TABLE_IDENTIFIER_KEYS = [:from, :join].freeze
      COLUMN_IDENTIFIER_KEYS = [:select, :where, :having, :order, :group, :compounds].freeze

      # Returns a hash of literal string identifier keys referenced by the given
      # dataset with the given dataset as the value for each key.
      def self.references(dataset)
        new(dataset).tap{|ext| ext.transform(dataset)}.references
      end

      attr_reader :references

      def initialize(dataset)
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
          TABLE_IDENTIFIER_KEYS.each{|k| o.opts[k]&.each{|jc| table_identifier_extract(jc)}}

          # Look in other keys that may have qualified references or subqueries
          COLUMN_IDENTIFIER_KEYS.each{|k| v(o.opts[k])}
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
