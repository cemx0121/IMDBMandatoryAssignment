namespace IMDBLib
{
    public class Title
    {
        public string TConst { get; set; }
        public string TitleType { get; set; }

        public int TitleTypeID { get; set; }
        public string PrimaryTitle { get; set; }
        public string OriginalTitle { get; set; }
        public bool IsAdult { get; set; }
        public int? StartYear { get; set; }
        public int? EndYear { get; set; }
        public int? RuntimeMinutes { get; set; }
        public string[] Genres { get; set; }

    }
}