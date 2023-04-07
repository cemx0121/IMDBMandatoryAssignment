using IMDBLib;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

public class IMDBNameDataProcessor
{
    private readonly SqlConnection _connection;
    private DataTable _namesTable;
    private DataTable _nameProfessionsTable;
    private DataTable _nameKnownForTitlesTable;
    private Dictionary<string, int> _professionCache;

    public IMDBNameDataProcessor(SqlConnection connection)
    {
        _connection = connection;
        InitializeTables();
        _professionCache = new Dictionary<string, int>();
    }

    private void InitializeTables()
    {
        _namesTable = new DataTable();
        _namesTable.Columns.Add("Nconst", typeof(string));
        _namesTable.Columns.Add("PrimaryName", typeof(string));
        _namesTable.Columns.Add("BirthYear", typeof(int));
        _namesTable.Columns.Add("DeathYear", typeof(int));

        _nameProfessionsTable = new DataTable();
        _nameProfessionsTable.Columns.Add("Nconst", typeof(string));
        _nameProfessionsTable.Columns.Add("ProfessionID", typeof(int));

        _nameKnownForTitlesTable = new DataTable();
        _nameKnownForTitlesTable.Columns.Add("Nconst", typeof(string));
        _nameKnownForTitlesTable.Columns.Add("Tconst", typeof(string));
    }

    public void ProcessDataStreaming(string dataFilePath)
    {
        ReadDataFile(dataFilePath);
        PerformBulkInsertions();
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
        var name = new Name
        {
            NConst = row[0],
            PrimaryName = row[1],
            BirthYear = row[2] != @"\N" ? int.Parse(row[2]) : (int?)null,
            DeathYear = row[3] != @"\N" ? int.Parse(row[3]) : (int?)null,
            Professions = row[4] != @"\N" ? row[4].Split(',') : new string[0],
            KnownForTitles = row[5] != @"\N" ? row[5].Split(',') : new string[0]
        };

        ProcessName(name);
    }

    private void ProcessName(Name name)
    {
        DataRow nameRow = _namesTable.NewRow();
        nameRow["Nconst"] = name.NConst;
        nameRow["PrimaryName"] = name.PrimaryName;
        nameRow["BirthYear"] = name.BirthYear.HasValue ? (object)name.BirthYear.Value : DBNull.Value;
        nameRow["DeathYear"] = name.DeathYear.HasValue ? (object)name.DeathYear.Value : DBNull.Value;
        _namesTable.Rows.Add(nameRow);

        foreach (string professionName in name.Professions)
        {
            if (!_professionCache.TryGetValue(professionName, out int professionId))
            {
                using (var command = new SqlCommand("SELECT ProfessionID FROM Professions WHERE Profession = @Profession", _connection))
                {
                    command.Parameters.AddWithValue("@Profession", professionName);
                    professionId = (int?)command.ExecuteScalar() ?? -1;
                }

                if (professionId == -1)
                {
                    using (var insertCommand = new SqlCommand("INSERT INTO Professions (Profession) VALUES (@Profession); SELECT SCOPE_IDENTITY();", _connection))
                    {
                        insertCommand.Parameters.AddWithValue("@Profession", professionName);
                        professionId = Convert.ToInt32(insertCommand.ExecuteScalar());
                    }

                    Console.WriteLine($"Inserted new profession '{professionName}' with ProfessionID {professionId}.");
                }

                _professionCache.Add(professionName, professionId);
            }

            _nameProfessionsTable.Rows.Add(name.NConst, professionId);
        }

        foreach (string tconst in name.KnownForTitles)
        {
            DataRow knownForRow = _nameKnownForTitlesTable.NewRow();
            knownForRow["Nconst"] = name.NConst;
            knownForRow["Tconst"] = tconst;
            _nameKnownForTitlesTable.Rows.Add(knownForRow);
        }
    }

    private void PerformBulkInsertions()
    {
        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
        {
            bulkCopy.DestinationTableName = "Names";
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.BatchSize = 5000;
            bulkCopy.NotifyAfter = 10000;
            bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
            Console.WriteLine("Inserting names...");
            bulkCopy.WriteToServer(_namesTable);
            Console.WriteLine("Names inserted.");
        }

        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
        {
            bulkCopy.DestinationTableName = "NameProfessions";
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.BatchSize = 5000;
            bulkCopy.ColumnMappings.Add("Nconst", "Nconst");
            bulkCopy.ColumnMappings.Add("ProfessionID", "ProfessionID");
            bulkCopy.NotifyAfter = 10000;
            bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
            Console.WriteLine("Inserting NameProfessions...");
            bulkCopy.WriteToServer(_nameProfessionsTable);
            Console.WriteLine("NameProfessions inserted.");
        }

        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
        {
            bulkCopy.DestinationTableName = "NameKnownForTitles";
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.BatchSize = 5000;
            bulkCopy.ColumnMappings.Add("Nconst", "Nconst");
            bulkCopy.ColumnMappings.Add("Tconst", "Tconst");
            bulkCopy.NotifyAfter = 10000;
            bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
            Console.WriteLine("Inserting KnownForTitles...");
            bulkCopy.WriteToServer(_nameKnownForTitlesTable);
            Console.WriteLine("KnownForTitles inserted.");
        }
    }

    private static void BulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
    {
        Console.WriteLine("Rows Copied: {0}", e.RowsCopied);
    }
}
