using MongoDB.Driver;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoApi.Models
{
    public class VisualizacionService : IVisualizacionService
    {
        private readonly IMongoCollection<Visualizacion> _visualizaciones;
        private readonly IMongoCollection<Movie> _movies;
        private readonly IMongoCollection<Usuario> _usuarios;

        public VisualizacionService(IOptions<TodoDatabaseSettings> settings, IMongoClient client)
        {
            var database = client.GetDatabase(settings.Value.DatabaseName);
            _visualizaciones = database.GetCollection<Visualizacion>(settings.Value.VisualizacionCollectionName);
            _movies = database.GetCollection<Movie>(settings.Value.MoviesCollectionName);
            _usuarios = database.GetCollection<Usuario>("Usuarios");
        }

        public async Task<Usuario> GetUsuarioAsync(string idUsuario)
        {
            var filter = Builders<Usuario>.Filter.Eq(u => u.Id, idUsuario);
            return await _usuarios.Find(filter).FirstOrDefaultAsync();
        }

        public async Task<List<Visualizacion>> GetAsync() =>
            await _visualizaciones.Find(_ => true).ToListAsync();

        public async Task<Visualizacion?> GetAsync(string id) =>
            await _visualizaciones.Find(v => v.Id == id).FirstOrDefaultAsync();

        public async Task CreateAsync(Visualizacion visualizacion)
        {
            var comen = false;

                // Verifica si 'Comentario' no es nulo ni vacío
                if (!string.IsNullOrWhiteSpace(visualizacion.Comentario))
                {
                    comen = true;
                }
             visualizacion.Sumatoria = Convert.ToInt32(visualizacion.Like) +
                                 Convert.ToInt32(comen) +
                                 Convert.ToInt32(visualizacion.Compartir) +
                                 Convert.ToInt32(visualizacion.Repetir);
            await _visualizaciones.InsertOneAsync(visualizacion);
            await UpdateMovieStatsAsync(visualizacion);
        }

        public async Task UpdateAsync(string id, Visualizacion updatedVisualizacion) =>
            await _visualizaciones.ReplaceOneAsync(v => v.Id == id, updatedVisualizacion);

        public async Task RemoveAsync(string id) =>
            await _visualizaciones.DeleteOneAsync(v => v.Id == id);

        private async Task UpdateMovieStatsAsync(Visualizacion visualizacion)
        {
            var filter = Builders<Movie>.Filter.Eq(m => m.MovieId , visualizacion.IdMovie);
            var update = Builders<Movie>.Update
                .Inc(m => m.Views, 1)
                .Inc(m => m.Likes, visualizacion.Like ? 1 : 0)
                .Inc(m => m.Dislikes, visualizacion.Like ? 0 : 1)
                .Inc(m => m.AverageView, visualizacion.Rating); // Necesitarás una mejor lógica para el promedio real

            await _movies.UpdateOneAsync(filter, update);
        }
    }

    public interface IVisualizacionService
    {
        Task<List<Visualizacion>> GetAsync();
        Task<Visualizacion?> GetAsync(string id);
        Task CreateAsync(Visualizacion visualizacion);
        Task UpdateAsync(string id, Visualizacion updatedVisualizacion);
        Task RemoveAsync(string id);
        Task<Usuario> GetUsuarioAsync(string idUsuario);
    }
}
