using IMDBConsoleApp.DataProcessor;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBConsoleApp.DataInserters
{
    public class IMDBTitleCrewDataBunkInserter
    {
        private readonly string _connectionString;

        public IMDBTitleCrewDataBunkInserter(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void ImportData(string dataFilePath)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var processor = new IMDBTitleCrewDataProcessor(connection);
                processor.ProcessTitleCrewDataStreaming(dataFilePath);
            }
        }
    }
}
