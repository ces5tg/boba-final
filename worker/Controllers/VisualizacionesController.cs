using Microsoft.AspNetCore.Mvc;
using TodoApi.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class VisualizacionController : ControllerBase
    {
        private readonly IVisualizacionService _visualizacionService;

        public VisualizacionController(IVisualizacionService visualizacionService)
        {
            _visualizacionService = visualizacionService;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Visualizacion>>> GetVisualizaciones()
        {
            var visualizaciones = await _visualizacionService.GetAsync();
            return Ok(visualizaciones);
        }

        [HttpGet("{id:length(24)}", Name = "GetVisualizacion")]
        public async Task<ActionResult<Visualizacion>> GetVisualizacion(string id)
        {
            var visualizacion = await _visualizacionService.GetAsync(id);

            if (visualizacion == null)
            {
                return NotFound();
            }

            return Ok(visualizacion);
        }

        [HttpPost]
        public async Task<ActionResult<Visualizacion>> CreateVisualizacion(Visualizacion visualizacion)
        {
            await _visualizacionService.CreateAsync(visualizacion);
            return CreatedAtRoute("GetVisualizacion", new { id = visualizacion.Id }, visualizacion);
        }

        [HttpPut("{id:length(24)}")]
        public async Task<IActionResult> UpdateVisualizacion(string id, Visualizacion updatedVisualizacion)
        {
            var visualizacion = await _visualizacionService.GetAsync(id);

            if (visualizacion == null)
            {
                return NotFound();
            }

            updatedVisualizacion.Id = visualizacion.Id;
            await _visualizacionService.UpdateAsync(id, updatedVisualizacion);

            return NoContent();
        }

        [HttpDelete("{id:length(24)}")]
        public async Task<IActionResult> DeleteVisualizacion(string id)
        {
            var visualizacion = await _visualizacionService.GetAsync(id);

            if (visualizacion == null)
            {
                return NotFound();
            }

            await _visualizacionService.RemoveAsync(id);
            return NoContent();
        }

        [HttpGet("Usuario/{idUsuario}")]
        public async Task<ActionResult<Usuario>> GetUsuario(string idUsuario)
        {
            var usuario = await _visualizacionService.GetUsuarioAsync(idUsuario);

            if (usuario == null)
            {
                return NotFound();
            }

            return Ok(usuario);
        }
    }
}
