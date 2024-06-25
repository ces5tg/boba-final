using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.ComponentModel.DataAnnotations;

namespace TodoApi.Models
{
    public class Usuario
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = string.Empty;

        public string? Nombre { get; set; }
        public string? Gmail { get; set; } = string.Empty;  // Inicializar con cadena vacía
        public string? Password { get; set; } = string.Empty;  // Inicializar con cadena vacía
        public List<string>? CategoriasPreferidas { get; set; }
        
    }
    

    public class LoginRequest
    {
        [Required(ErrorMessage = "El campo Gmail es obligatorio.")]
        public required string Gmail { get; set; }

        [Required(ErrorMessage = "El campo Password es obligatorio.")]
        public required string Password { get; set; }
    }
}
