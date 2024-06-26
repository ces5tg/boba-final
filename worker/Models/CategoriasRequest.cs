
using System;
using System.Collections.Generic;

namespace TodoApi.Models
{
   public class CategoriaRequest
{
    public string Id { get; set; }
    public List<string> CategoriasPreferidas { get; set; }
}
}