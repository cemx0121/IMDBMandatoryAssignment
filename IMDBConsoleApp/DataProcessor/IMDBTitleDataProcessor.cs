using IMDBLib;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBConsoleApp.DataProcessor
{
    public class IMDBTitleDataProcessor
    {
        private readonly SqlConnection _connection;
        private Dictionary<string, int> _titleTypes;
        private Dictionary<string, int> _genres;
        private DataTable _titlesTable;
        private DataTable _titleGenresTable;

        public IMDBTitleDataProcessor(SqlConnection connection)
        {
            _connection = connection;
            InitializeTables();
        }

        private void InitializeTables()
        {
            _titleTypes = new Dictionary<string, int>();
            _genres = new Dictionary<string, int>();

            _titlesTable = new DataTable();
            _titlesTable.Columns.Add("Tconst", typeof(string));
            _titlesTable.Columns.Add("TitleTypeID", typeof(int));
            _titlesTable.Columns.Add("PrimaryTitle", typeof(string));
            _titlesTable.Columns.Add("OriginalTitle", typeof(string));
            _titlesTable.Columns.Add("IsAdult", typeof(bool));
            _titlesTable.Columns.Add("StartYear", typeof(int));
            _titlesTable.Columns.Add("EndYear", typeof(int));
            _titlesTable.Columns.Add("RunTimeMinutes", typeof(int));

            _titleGenresTable = new DataTable();
            _titleGenresTable.Columns.Add("Tconst", typeof(string));
            _titleGenresTable.Columns.Add("GenreID", typeof(int));
        }

        public void ProcessDataStreaming(string dataFilePath)
        {
            LoadExistingTitleTypesAndGenres();
            ReadDataFile(dataFilePath);
            PerformBulkInsertions();
        }

        private void LoadExistingTitleTypesAndGenres()
        {
            using (var command = new SqlCommand("SELECT TitleTypeID, TitleType FROM TitleTypes", _connection))
            using (var reader = command.ExecuteReader())
                while (reader.Read())
                    _titleTypes[reader.GetString(1)] = reader.GetInt32(0);

            using (var command = new SqlCommand("SELECT GenreID, Genre FROM Genres", _connection))
            using (var reader = command.ExecuteReader())
                while (reader.Read())
                    _genres[reader.GetString(1)] = reader.GetInt32(0);
        }

        private void ReadDataFile(string dataFilePath)
        {
            using (var reader = new StreamReader(dataFilePath))
            {
                string line;
                int lineCounter = 0;

                while ((line = reader.ReadLine()) != null)
                {
                    if (lineCounter > 0) // Skip header line
                    {
                        ProcessLine(line);
                    }

                    lineCounter++;
                }
            }
        }

        private void ProcessLine(string line)
        {
            var row = line.Split('\t');
            var title = new Title
            {
                TConst = row[0],
                TitleType = row[1],
                PrimaryTitle = row[2],
                OriginalTitle = row[3],
                IsAdult = row[4] == "1",
                StartYear = row[5] != @"\N" ? int.Parse(row[5]) : (int?)null,
                EndYear = row[6] != @"\N" ? int.Parse(row[6]) : (int?)null,
                RuntimeMinutes = row[7] != @"\N" ? int.Parse(row[7]) : (int?)null,
                Genres = row[8] != @"\N" ? row[8].Split(',') : new string[0]
            };

            ProcessTitle(title);
        }

        private void ProcessTitle(Title title)
        {
            if (!_titleTypes.ContainsKey(title.TitleType))
            {
                using (var command = new SqlCommand("INSERT INTO TitleTypes (TitleType) VALUES (@TitleType); SELECT SCOPE_IDENTITY()", _connection))
                {
                    command.Parameters.AddWithValue("@TitleType", title.TitleType);
                    int newTitleTypeId = Convert.ToInt32(command.ExecuteScalar());
                    _titleTypes[title.TitleType] = newTitleTypeId;
                }
            }

            int titleTypeId = _titleTypes[title.TitleType];
            DataRow titleRow = _titlesTable.NewRow();
            titleRow["Tconst"] = title.TConst;
            titleRow["TitleTypeID"] = titleTypeId;
            titleRow["PrimaryTitle"] = title.PrimaryTitle;
            titleRow["OriginalTitle"] = title.OriginalTitle;
            titleRow["IsAdult"] = title.IsAdult;
            titleRow["StartYear"] = title.StartYear.HasValue ? (object)title.StartYear.Value : DBNull.Value;
            titleRow["EndYear"] = title.EndYear.HasValue ? (object)title.EndYear.Value : DBNull.Value;
            titleRow["RunTimeMinutes"] = title.RuntimeMinutes.HasValue ? (object)title.RuntimeMinutes.Value : DBNull.Value;
            _titlesTable.Rows.Add(titleRow);

            foreach (string genreName in title.Genres)
            {
                if (!_genres.ContainsKey(genreName))
                {
                    using (var command = new SqlCommand("INSERT INTO Genres (Genre) VALUES (@Genre); SELECT SCOPE_IDENTITY()", _connection))
                    {
                        command.Parameters.AddWithValue("@Genre", genreName);
                        int newGenreId = Convert.ToInt32(command.ExecuteScalar());
                        _genres[genreName] = newGenreId;
                    }
                }

                int genreId;
                if (_genres.TryGetValue(genreName, out genreId))
                {
                    _titleGenresTable.Rows.Add(title.TConst, genreId);
                }
                else
                {
                    Console.WriteLine($"Error: Unable to find GenreID for genre '{genreName}'.");
                }
            }
        }

        private void PerformBulkInsertions()
        {
            using (var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "Titles";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.BatchSize = 5000;
                bulkCopy.NotifyAfter = 10000;
                bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
                Console.WriteLine("Inserting titles...");
                bulkCopy.WriteToServer(_titlesTable);
                Console.WriteLine("Titles inserted.");
            }

            using (var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "TitleGenres";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.BatchSize = 5000;
                bulkCopy.ColumnMappings.Add("Tconst", "Tconst"); 
                bulkCopy.ColumnMappings.Add("GenreID", "GenreID");
                bulkCopy.NotifyAfter = 10000;
                bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
                Console.WriteLine("Inserting TitleGenres....");
                bulkCopy.WriteToServer(_titleGenresTable);
                Console.WriteLine("TitleGenres inserted.");
            }
        }

        private static void BulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Rows Copied: {0}", e.RowsCopied);
        }
    }
}
