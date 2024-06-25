using MongoDB.Driver;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoApi.Models
{
    public class MovieService : IMovieService
    {
        private readonly IMongoCollection<Movie> _movies;

        public MovieService(IOptions<TodoDatabaseSettings> settings, IMongoClient client)
        {
            var database = client.GetDatabase(settings.Value.DatabaseName);
            _movies = database.GetCollection<Movie>(settings.Value.MoviesCollectionName);
        }

        public async Task<List<Movie>> GetAsync() =>
            await _movies.Find(movie => true).ToListAsync();

        public async Task<Movie?> GetAsync(string id) =>
            await _movies.Find<Movie>(movie => movie.Id == id).FirstOrDefaultAsync();

        public async Task CreateAsync(Movie movie) =>
            await _movies.InsertOneAsync(movie);

        public async Task UpdateAsync(string id, Movie updatedMovie) =>
            await _movies.ReplaceOneAsync(movie => movie.Id == id, updatedMovie);

        public async Task RemoveAsync(string id) =>
            await _movies.DeleteOneAsync(movie => movie.Id == id);
    }

    public interface IMovieService
    {
        Task<List<Movie>> GetAsync();
        Task<Movie?> GetAsync(string id);
        Task CreateAsync(Movie movie);
        Task UpdateAsync(string id, Movie updatedMovie);
        Task RemoveAsync(string id);
    }
}
