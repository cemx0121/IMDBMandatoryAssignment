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
    public class IMDBTitleCrewDataProcessor
    {
        private readonly SqlConnection _connection;
        private DataTable _titleDirectorsTable;
        private DataTable _titleWritersTable;

        public IMDBTitleCrewDataProcessor(SqlConnection connection)
        {
            _connection = connection;
            InitializeTables();
        }

        private void InitializeTables()
        {
            _titleDirectorsTable = new DataTable();
            _titleDirectorsTable.Columns.Add("Tconst", typeof(string));
            _titleDirectorsTable.Columns.Add("Nconst", typeof(string));

            _titleWritersTable = new DataTable();
            _titleWritersTable.Columns.Add("Tconst", typeof(string));
            _titleWritersTable.Columns.Add("Nconst", typeof(string));
        }

        public void ProcessTitleCrewDataStreaming(string dataFilePath)
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
            var tconst = row[0];
            var directors = row[1] != @"\N" ? row[1].Split(',') : new string[0];
            var writers = row[2] != @"\N" ? row[2].Split(',') : new string[0];

            foreach (string directorNconst in directors)
            {
                DataRow titleDirectorsRow = _titleDirectorsTable.NewRow();
                titleDirectorsRow["Tconst"] = tconst;
                titleDirectorsRow["Nconst"] = directorNconst;
                _titleDirectorsTable.Rows.Add(titleDirectorsRow);
            }

            foreach (string writerNconst in writers)
            {
                DataRow titleWritersRow = _titleWritersTable.NewRow();
                titleWritersRow["Tconst"] = tconst;
                titleWritersRow["Nconst"] = writerNconst;
                _titleWritersTable.Rows.Add(titleWritersRow);
            }
        }

        private void PerformBulkInsertions()
        {
            using (var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "TitleDirectors";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.BatchSize = 5000;
                bulkCopy.ColumnMappings.Add("Tconst", "Tconst");
                bulkCopy.ColumnMappings.Add("Nconst", "Nconst");
                bulkCopy.NotifyAfter = 10000;
                bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
                Console.WriteLine("Inserting TitleDirectors...");
                bulkCopy.WriteToServer(_titleDirectorsTable);
                Console.WriteLine("TitleDirectors inserted.");
            }

            using (var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "TitleWriters";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.BatchSize = 5000;
                bulkCopy.ColumnMappings.Add("Tconst", "Tconst");
                bulkCopy.ColumnMappings.Add("Nconst", "Nconst");
                bulkCopy.NotifyAfter = 10000;
                bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkCopy_SqlRowsCopied);
                Console.WriteLine("Inserting TitleWriters...");
                bulkCopy.WriteToServer(_titleWritersTable);
                Console.WriteLine("TitleWriters inserted.");
            }
        }

        private static void BulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Rows Copied: {0}", e.RowsCopied);
        }
    }
}
