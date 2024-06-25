using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace TodoApi.Models
{
    public class Visualizacion
    {
         [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = string.Empty;

        [BsonRepresentation(BsonType.ObjectId)]

        public string IdUsuario { get; set; } = default!;
        //public string  movieId{ get; set; }  // IDENTIFICADOR NUMERICO 
        public string IdMovie { get; set; } = default!; // Indetificador numerioco
        public TimeSpan TimeVisualizacion { get; set; }
        public bool Like { get; set; }
        public int Rating { get; set; }
        public string? Comentario { get; set; }
        public bool Compartir { get; set; }
        public bool Repetir { get; set; }
        public int Sumatoria { get; set; }
        public DateTime Fecha { get; set; } = DateTime.UtcNow;
        public DateTime FechaVisualizacion { get; set; } = DateTime.UtcNow;
    }
}
