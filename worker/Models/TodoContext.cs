using Microsoft.EntityFrameworkCore;

namespace TodoApi.Models
{
    public class TodoContext : DbContext
    {
        public TodoContext(DbContextOptions<TodoContext> options)
            : base(options)
        {
        }

        
        public DbSet<Movie> Movies { get; set; } = null!;

        public DbSet<Usuario> Usuarios { get; set; } = null!;
    }
}
