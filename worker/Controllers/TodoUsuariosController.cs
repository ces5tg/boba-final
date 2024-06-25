using Microsoft.AspNetCore.Mvc;
using TodoApi.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsuariosController : ControllerBase
    {
        private readonly IUsuarioService _usuarioService;

        public UsuariosController(IUsuarioService usuarioService)
        {
            _usuarioService = usuarioService;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Usuario>>> GetUsuarios()
        {
            var usuarios = await _usuarioService.GetAsync();
            return Ok(usuarios);
        }

        [HttpGet("{id:length(24)}", Name = "GetUsuario")]
        public async Task<ActionResult<Usuario>> GetUsuario(string id)
        {
            var usuario = await _usuarioService.GetAsync(id);

            if (usuario == null)
            {
                return NotFound();
            }

            return Ok(usuario);
        }

        [HttpPost]
        public async Task<ActionResult<Usuario>> CreateUsuario(Usuario usuario)
        {
            await _usuarioService.CreateAsync(usuario);
            return CreatedAtRoute("GetUsuario", new { id = usuario.Id }, usuario);
        }

        [HttpPut("{id:length(24)}")]
        public async Task<IActionResult> UpdateUsuario(string id, Usuario usuarioIn)
        {
            var usuario = await _usuarioService.GetAsync(id);

            if (usuario == null)
            {
                return NotFound();
            }

            usuarioIn.Id = usuario.Id;
            await _usuarioService.UpdateAsync(id, usuarioIn);

            return NoContent();
        }

        [HttpDelete("{id:length(24)}")]
        public async Task<IActionResult> DeleteUsuario(string id)
        {
            var usuario = await _usuarioService.GetAsync(id);

            if (usuario == null)
            {
                return NotFound();
            }

            await _usuarioService.RemoveAsync(id);

            return NoContent();
        }

        [HttpPost("login")]
        public async Task<ActionResult<Usuario>> Login([FromBody] LoginRequest request)
        {
            var usuario = await _usuarioService.AuthenticateAsync(request.Gmail, request.Password);

            if (usuario == null)
            {
                Console.WriteLine("Inicio de sesión fallido: Credenciales incorrectas"); // Agregar comentario
                return Unauthorized();
            }

            Console.WriteLine($"Inicio de sesión exitoso: Bienvenido, {usuario.Nombre}"); // Agregar comentario

            return Ok(usuario);
        }
    }
}
