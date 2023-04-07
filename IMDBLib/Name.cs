using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBLib
{
    public class Name
    {
        public string NConst { get; set; }
        public string PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public string[] Professions { get; set; }
        public string[] KnownForTitles { get; set; }
    }
}
