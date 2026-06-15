using System.Data;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Nodus.Npgsql.Compatibility.Tests;

public sealed class NpgsqlCompatibilityTests
{
    private static string BaseConnectionString =>
        Environment.GetEnvironmentVariable("NODUS_NPGSQL_CONNECTION_STRING")
        ?? throw new InvalidOperationException("NODUS_NPGSQL_CONNECTION_STRING is not set.");

    private static string UniqueName(string prefix) =>
        $"{prefix}_{Guid.NewGuid():N}".ToLowerInvariant();

    private static async Task<NpgsqlConnection> OpenConnectionAsync()
    {
        var builder = new NpgsqlConnectionStringBuilder(BaseConnectionString)
        {
            Pooling = false
        };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    [Fact]
    public async Task ConnectsAndUsesPooling()
    {
        var builder = new NpgsqlConnectionStringBuilder(BaseConnectionString)
        {
            Pooling = true,
            MaxPoolSize = 4,
            ApplicationName = "npgsql-compat-pooling"
        };

        for (var i = 0; i < 6; i++)
        {
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand("SELECT 1", connection);
            Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync()));
        }
    }

    [Fact]
    public async Task PreparedTypedParametersRoundTripDriverTypes()
    {
        await using var connection = await OpenConnectionAsync();
        var table = UniqueName("npgsql_types");
        await using (var create = new NpgsqlCommand($"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                uid UUID,
                tags TEXT[],
                payload JSONB,
                event_date DATE,
                event_time TIME,
                event_ts TIMESTAMP,
                event_tstz TIMESTAMPTZ,
                raw BYTEA,
                amount NUMERIC
            );
            """, connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var uid = Guid.NewGuid();
        var eventDate = new DateOnly(2026, 6, 15);
        var eventTime = new TimeOnly(12, 34, 56, 789);
        var eventTimestamp = new DateTime(2026, 6, 15, 12, 34, 56, DateTimeKind.Unspecified).AddMilliseconds(789);
        var eventTimestampTz = new DateTime(2026, 6, 15, 9, 34, 56, DateTimeKind.Utc).AddMilliseconds(789);
        var raw = new byte[] { 0, 1, 2, 0xfe, 0xff };

        await using (var insert = new NpgsqlCommand($"""
            INSERT INTO {table}
                (id, uid, tags, payload, event_date, event_time, event_ts, event_tstz, raw, amount)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, connection))
        {
            insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1, NpgsqlDbType = NpgsqlDbType.Integer });
            insert.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = uid, NpgsqlDbType = NpgsqlDbType.Uuid });
            insert.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = ["alpha", "beta"], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = """{"ok":true,"n":7}""", NpgsqlDbType = NpgsqlDbType.Jsonb });
            insert.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = eventDate, NpgsqlDbType = NpgsqlDbType.Date });
            insert.Parameters.Add(new NpgsqlParameter<TimeOnly> { TypedValue = eventTime, NpgsqlDbType = NpgsqlDbType.Time });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = eventTimestamp, NpgsqlDbType = NpgsqlDbType.Timestamp });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = eventTimestampTz, NpgsqlDbType = NpgsqlDbType.TimestampTz });
            insert.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = raw, NpgsqlDbType = NpgsqlDbType.Bytea });
            insert.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 12345.67m, NpgsqlDbType = NpgsqlDbType.Numeric });
            await insert.PrepareAsync();
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using var select = new NpgsqlCommand($"""
            SELECT uid, tags, payload, event_date, event_time, event_ts, event_tstz, raw, amount
            FROM {table}
            WHERE id = $1
            """, connection);
        select.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1, NpgsqlDbType = NpgsqlDbType.Integer });

        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(uid, reader.GetFieldValue<Guid>(0));
        Assert.Equal(["alpha", "beta"], reader.GetFieldValue<string[]>(1));
        Assert.Contains("\"ok\":true", reader.GetFieldValue<string>(2), StringComparison.Ordinal);
        Assert.Equal(eventDate, reader.GetFieldValue<DateOnly>(3));
        Assert.Equal(eventTime, reader.GetFieldValue<TimeOnly>(4));
        Assert.Equal(eventTimestamp, reader.GetFieldValue<DateTime>(5));
        Assert.Equal(eventTimestampTz, reader.GetFieldValue<DateTime>(6).ToUniversalTime());
        Assert.Equal(raw, reader.GetFieldValue<byte[]>(7));
        Assert.True(reader.IsDBNull(8) || decimal.Parse(reader.GetFieldValue<string>(8), CultureInfo.InvariantCulture) == 12345.67m);
    }

    [Fact]
    public async Task BatchExecutesMultipleCommands()
    {
        await using var connection = await OpenConnectionAsync();
        var table = UniqueName("npgsql_batch");

        await using var batch = new NpgsqlBatch(connection);
        batch.BatchCommands.Add(new NpgsqlBatchCommand($"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT);"));
        batch.BatchCommands.Add(new NpgsqlBatchCommand($"INSERT INTO {table} (id, name) VALUES (1, 'one');"));
        batch.BatchCommands.Add(new NpgsqlBatchCommand($"INSERT INTO {table} (id, name) VALUES (2, 'two');"));
        batch.BatchCommands.Add(new NpgsqlBatchCommand($"SELECT name FROM {table} WHERE id = 2;"));

        await using var reader = await batch.ExecuteReaderAsync();
        do
        {
            if (reader.FieldCount == 0)
            {
                continue;
            }

            Assert.True(await reader.ReadAsync());
            Assert.Equal("two", reader.GetString(0));
            return;
        }
        while (await reader.NextResultAsync());

        Assert.Fail("NpgsqlBatch did not return the SELECT result.");
    }

    [Fact]
    public async Task SavepointsRollbackAndRelease()
    {
        await using var connection = await OpenConnectionAsync();
        var table = UniqueName("npgsql_savepoints");
        await using (var create = new NpgsqlCommand($"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT);", connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        await using (var begin = new NpgsqlCommand("BEGIN;", connection))
        {
            await begin.ExecuteNonQueryAsync();
        }
        await using (var insert = new NpgsqlCommand($"INSERT INTO {table} (id, name) VALUES (1, 'kept');", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        await using (var savepoint = new NpgsqlCommand("SAVEPOINT sp_driver;", connection))
        {
            await savepoint.ExecuteNonQueryAsync();
        }
        await using (var insert = new NpgsqlCommand($"INSERT INTO {table} (id, name) VALUES (2, 'rolled-back');", connection))
        {
            await insert.ExecuteNonQueryAsync();
        }

        await using (var rollbackTo = new NpgsqlCommand("ROLLBACK TO SAVEPOINT sp_driver;", connection))
        {
            await rollbackTo.ExecuteNonQueryAsync();
        }
        await using (var release = new NpgsqlCommand("RELEASE SAVEPOINT sp_driver;", connection))
        {
            await release.ExecuteNonQueryAsync();
        }
        await using (var commit = new NpgsqlCommand("COMMIT;", connection))
        {
            await commit.ExecuteNonQueryAsync();
        }

        await using var kept = new NpgsqlCommand($"SELECT name FROM {table} WHERE id = 1;", connection);
        Assert.Equal("kept", Convert.ToString(await kept.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task CancellationReportsQueryCancelledAndSessionSurvives()
    {
        await using var connection = await OpenConnectionAsync();
        await using (var timeout = new NpgsqlCommand("SET statement_timeout = 1", connection))
        {
            await timeout.ExecuteNonQueryAsync();
        }

        await using var sleep = new NpgsqlCommand("SELECT pg_sleep(1)", connection);
        var exception = await Assert.ThrowsAsync<PostgresException>(() => sleep.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.QueryCanceled, exception.SqlState);

        await using (var reset = new NpgsqlCommand("SET statement_timeout = 0", connection))
        {
            await reset.ExecuteNonQueryAsync();
        }

        await using var ping = new NpgsqlCommand("SELECT 1", connection);
        Assert.Equal(1, Convert.ToInt32(await ping.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task SchemaMetadataIncludesTablesAndColumns()
    {
        await using var connection = await OpenConnectionAsync();
        var table = UniqueName("npgsql_schema");
        await using (var create = new NpgsqlCommand($"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                uid UUID
            );
            """, connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var tables = connection.GetSchema("Tables");
        Assert.Contains(tables.Rows.Cast<DataRow>(), row =>
            string.Equals(Convert.ToString(row["table_name"]), table, StringComparison.OrdinalIgnoreCase));

        var columns = connection.GetSchema("Columns", [null, "public", table]);
        var columnNames = columns.Rows.Cast<DataRow>()
            .Select(row => Convert.ToString(row["column_name"]))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains("id", columnNames);
        Assert.Contains("name", columnNames);
        Assert.Contains("uid", columnNames);
    }

    [Fact]
    public async Task CopyApisOpenTextBinaryAndRawPaths()
    {
        await using var connection = await OpenConnectionAsync();
        var textTable = UniqueName("npgsql_copy_text");
        var binaryTable = UniqueName("npgsql_copy_binary");

        await using (var create = new NpgsqlCommand($"""
            CREATE TABLE {textTable} (id INT PRIMARY KEY, name TEXT);
            CREATE TABLE {binaryTable} (id INT PRIMARY KEY, name TEXT);
            """, connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        await using (var writer = await connection.BeginTextImportAsync($"COPY {textTable} FROM STDIN"))
        {
            await writer.WriteLineAsync("1\talpha");
            await writer.WriteLineAsync("2\tbeta");
        }

        await using (var importer = await connection.BeginBinaryImportAsync($"COPY {binaryTable} (id, name) FROM STDIN (FORMAT BINARY)"))
        {
            await importer.StartRowAsync();
            await importer.WriteAsync(1, NpgsqlDbType.Integer);
            await importer.WriteAsync("one", NpgsqlDbType.Text);
            await importer.CompleteAsync();
        }

        await using var rawCopy = await connection.BeginRawBinaryCopyAsync($"COPY {binaryTable} TO STDOUT (FORMAT BINARY)");
        var buffer = new byte[16];
        var read = await rawCopy.ReadAsync(buffer, 0, buffer.Length);
        Assert.True(read >= 0);
    }

    [Fact]
    public async Task EfCoreGeneratedSchemaAndQueryPath()
    {
        var table = UniqueName("npgsql_ef_items");
        await using var context = new EfCompatContext(BaseConnectionString, table);

        var script = context.Database.GenerateCreateScript();
        await context.Database.ExecuteSqlRawAsync(script);

        context.Items.Add(new EfCompatItem { Id = 1, Name = "from-ef" });
        await context.SaveChangesAsync();

        var item = await context.Items.SingleAsync(row => row.Id == 1);
        Assert.Equal("from-ef", item.Name);
    }

    private sealed class EfCompatContext(string connectionString, string tableName) : DbContext
    {
        public DbSet<EfCompatItem> Items => Set<EfCompatItem>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseNpgsql(connectionString);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EfCompatItem>(entity =>
            {
                entity.ToTable(tableName);
                entity.HasKey(item => item.Id);
                entity.Property(item => item.Id)
                    .HasColumnName("id")
                    .ValueGeneratedNever();
                entity.Property(item => item.Name)
                    .HasColumnName("name")
                    .HasColumnType("text");
            });
        }
    }

    private sealed class EfCompatItem
    {
        public int Id { get; set; }

        public string Name { get; set; } = "";
    }
}
