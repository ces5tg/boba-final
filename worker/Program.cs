using Microsoft.Extensions.Options;
using MongoDB.Driver;
using TodoApi.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Configurar servicios en el contenedor.
builder.Services.Configure<TodoDatabaseSettings>(
    builder.Configuration.GetSection(nameof(TodoDatabaseSettings)));

builder.Services.AddSingleton<IMongoClient>(s =>
    new MongoClient(builder.Configuration.GetValue<string>("TodoDatabaseSettings:ConnectionString")));

builder.Services.AddScoped<IUsuarioService, UsuarioService>();
builder.Services.AddScoped<IMovieService, MovieService>();
builder.Services.AddScoped<IVisualizacionService, VisualizacionService>();

// Añadir el servicio del consumidor de Kafka
builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.AddControllers();

// Configurar Swagger/OpenAPI
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configurar el pipeline de solicitudes HTTP.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

