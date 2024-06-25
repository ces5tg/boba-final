namespace TodoApi.Models
{
     

    public class TodoDatabaseSettings
    {
        public string ConnectionString { get; set; } = null!;
        public string DatabaseName { get; set; } = null!;
        public string MoviesCollectionName { get; set; } = null!;

        public string UsuariosCollectionName { get; set; } = null!;

        public string VisualizacionCollectionName { get; set; } = null!;
    }

    

  
}
