using IMDBConsoleApp.DataProcessor;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBConsoleApp.DataInserters
{
    public class IMDBTitleDataBunkInserter
    {
        private readonly string _connectionString;

        public IMDBTitleDataBunkInserter(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void ImportData(string dataFilePath)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var processor = new IMDBTitleDataProcessor(connection);
                processor.ProcessDataStreaming(dataFilePath);
            }
        }
    }
}
