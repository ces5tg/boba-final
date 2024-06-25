using MongoDB.Driver;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
namespace TodoApi.Models
{
    public class UsuarioService : IUsuarioService
    {
        private readonly IMongoCollection<Usuario> _usuarios;

        public UsuarioService(IOptions<TodoDatabaseSettings> settings, IMongoClient client)
        {
            var database = client.GetDatabase(settings.Value.DatabaseName);
            _usuarios = database.GetCollection<Usuario>(settings.Value.UsuariosCollectionName);
        }

        public async Task<List<Usuario>> GetAsync() =>
            await _usuarios.Find(usuario => true).ToListAsync();

        public async Task<Usuario?> GetAsync(string id) =>
            await _usuarios.Find<Usuario>(usuario => usuario.Id == id).FirstOrDefaultAsync();

        public async Task CreateAsync(Usuario usuario) =>
            await _usuarios.InsertOneAsync(usuario);

        public async Task UpdateAsync(string id, Usuario updatedUsuario) =>
            await _usuarios.ReplaceOneAsync(usuario => usuario.Id == id, updatedUsuario);

        public async Task RemoveAsync(string id) =>
            await _usuarios.DeleteOneAsync(usuario => usuario.Id == id);

        public async Task<Usuario?> AuthenticateAsync(string gmail, string password)
        {
            var usuario = await _usuarios.Find(u => u.Gmail == gmail).FirstOrDefaultAsync();

            // Si no se encontró el usuario con el correo electrónico dado, retornar null
            if (usuario == null)
                return null;

            // Aquí deberías comparar la contraseña ingresada con la almacenada de forma segura
            // En este ejemplo, se compara como texto plano (lo cual no es seguro)
            if (usuario.Password == password)
                return usuario;
            else
                return null;
        }
    }

     

    public interface IUsuarioService
    {
        Task<List<Usuario>> GetAsync();
        Task<Usuario?> GetAsync(string id);
        Task CreateAsync(Usuario usuario);
        Task UpdateAsync(string id, Usuario updatedUsuario);
        Task RemoveAsync(string id);
        Task<Usuario?> AuthenticateAsync(string gmail, string password);
    }
}
