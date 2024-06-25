using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TodoApi.Models;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly string _topic;
    private readonly string _bootstrapServers;
    private readonly HttpClient _httpClient;

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;
        _topic = "ideas2";  // Cambia esto al nombre de tu tópico
        _bootstrapServers = "kafka-broker-1:9092";  // Cambia esto a la dirección de tu servidor Kafka

        _httpClient = new HttpClient();
        _httpClient.BaseAddress = new Uri("http://localhost:8080"); // Reemplaza con la URL base de tu API
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-group",
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    _logger.LogInformation($"Mensaje consumido: {consumeResult.Message.Value}");

                    // Deserializar el mensaje JSON
                    var json = consumeResult.Message.Value;
                    var visualizacion = JsonConvert.DeserializeObject<Visualizacion>(json);
 // Asegúrate de que el Id no se asigne manualmente si no es necesario
                        visualizacion.Id = MongoDB.Bson.ObjectId.GenerateNewId().ToString();
                    // Enviar datos a la API
                    EnviarVisualizacionAsync(visualizacion).GetAwaiter().GetResult();
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error al procesar mensaje de Kafka: {ex.Message}");
                throw;
            }
        }, stoppingToken);
    }

    private async Task EnviarVisualizacionAsync(Visualizacion visualizacion)
    {
        try
        {
            var json = JsonConvert.SerializeObject(visualizacion);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync("/api/Visualizacion", content);

            response.EnsureSuccessStatusCode(); // Asegura que la solicitud fue exitosa
            var responseString = await response.Content.ReadAsStringAsync();
            _logger.LogInformation($"Visualizacion insertada en API: {responseString}");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error al enviar visualización a la API: {ex.Message}");
        }
    }
}
