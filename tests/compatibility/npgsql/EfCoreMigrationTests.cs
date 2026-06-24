using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace Nodus.Npgsql.Compatibility.Tests;

/// <summary>
/// Exercises the Npgsql Entity Framework Core provider end-to-end: the provider
/// turns a code-first model into schema DDL (the heart of an EF migration) and
/// then translates LINQ into the SQL NodusDB has to answer.
///
/// Rather than <c>EnsureCreated</c>/<c>Migrate</c> — which key off whether the
/// shared <c>default</c> database already has tables, and whose
/// <c>EnsureDeleted</c> counterpart would drop the entire database — the test
/// drives the same model-to-DDL path through <see
/// cref="Microsoft.EntityFrameworkCore.RelationalDatabaseFacadeExtensions.GenerateCreateScript"/>
/// and applies it to a uniquely named table, so it is safe to run alongside the
/// other compatibility suites.
/// </summary>
public sealed class EfCoreMigrationTests
{
    private static string BaseConnectionString =>
        Environment.GetEnvironmentVariable("NODUS_NPGSQL_CONNECTION_STRING")
        ?? throw new InvalidOperationException("NODUS_NPGSQL_CONNECTION_STRING is not set.");

    public sealed class Product
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public int Stock { get; set; }
        public bool Active { get; set; }
    }

    private sealed class CatalogContext : DbContext
    {
        private readonly string _table;

        public CatalogContext(string table) => _table = table;

        public DbSet<Product> Products => Set<Product>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            var connectionString = new NpgsqlConnectionStringBuilder(BaseConnectionString)
            {
                Pooling = false
            }.ConnectionString;
            options.UseNpgsql(connectionString);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var entity = modelBuilder.Entity<Product>();
            entity.ToTable(_table);
            entity.HasKey(p => p.Id);
            // Client-assigned key so the DDL is a plain `uuid` column, not an
            // identity/serial column NodusDB does not model.
            entity.Property(p => p.Id).ValueGeneratedNever();
        }
    }

    [Fact]
    public async Task ModelToDdlThenCrudThroughEfCore()
    {
        var table = $"ef_products_{Guid.NewGuid():N}".ToLowerInvariant();
        await using var context = new CatalogContext(table);

        // The provider renders the model as schema DDL — exactly what an EF
        // migration's Up() applies — and we run it against NodusDB.
        var createScript = context.Database.GenerateCreateScript();
        await context.Database.ExecuteSqlRawAsync(createScript);

        try
        {
            var widget = new Product { Id = Guid.NewGuid(), Name = "Widget", Stock = 10, Active = true };
            var gadget = new Product { Id = Guid.NewGuid(), Name = "Gadget", Stock = 3, Active = false };
            context.Products.AddRange(widget, gadget);
            Assert.Equal(2, await context.SaveChangesAsync());

            // LINQ -> SQL: filter (bool + int) + order.
            var inStock = await context.Products
                .Where(p => p.Active && p.Stock >= 5)
                .OrderBy(p => p.Name)
                .ToListAsync();
            Assert.Single(inStock);
            Assert.Equal("Widget", inStock[0].Name);

            // Update through change tracking, then reload with a WHERE filter.
            gadget.Stock = 20;
            Assert.Equal(1, await context.SaveChangesAsync());
            var reloaded = await context.Products.AsNoTracking().SingleAsync(p => p.Id == gadget.Id);
            Assert.Equal(20, reloaded.Stock);

            // Delete through change tracking; materialize the remainder client-side
            // (EF's Count/Sum translate to cast-in-projection SQL NodusDB does not
            // yet plan, so we assert over the fetched rows instead).
            context.Products.Remove(widget);
            Assert.Equal(1, await context.SaveChangesAsync());
            var remaining = await context.Products.AsNoTracking().ToListAsync();
            Assert.Single(remaining);
            Assert.Equal(gadget.Id, remaining[0].Id);
        }
        finally
        {
            // Scoped cleanup: drop just this test's table (never the database).
            try
            {
                await context.Database.ExecuteSqlRawAsync($"DROP TABLE IF EXISTS {table}");
            }
            catch
            {
                // A leaked uniquely-named table is harmless to other tests.
            }
        }
    }
}
