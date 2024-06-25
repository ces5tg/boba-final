using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace TodoApi.Models
{
    public class Movie
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = string.Empty;
        
        public string MovieId   { get; set; }
        public string? Title { get; set; }
        public List<string>? Generos { get; set; }
        public string? Url { get; set; }
        public int Views { get; set; }
        public double AverageView { get; set; }  // Promedio de visualización
        public double ViewPercentage { get; set; }  // Porcentaje de vistas de la película
        public int Likes { get; set; }  // Suma de likes
        public int Dislikes { get; set; }  // Suma de dislikes
        public DateTime Date { get; set; } = DateTime.UtcNow;  // Fecha de la película
    }
}
