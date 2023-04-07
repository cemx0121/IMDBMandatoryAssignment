using IMDBLib;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB_UI_ConsoleApp.Services
{
    public class IMDBService
    {
        private readonly string _connectionString;

        public IMDBService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public List<Title> SearchTitle(string searchTerm)
        {
            var titles = new List<Title>();

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("TitleSearch", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@searchTerm", searchTerm);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            titles.Add(new Title
                            {
                                TConst = reader.GetString(0),
                                PrimaryTitle = reader.IsDBNull(1) ? null : reader.GetString(1),
                                OriginalTitle = reader.IsDBNull(2) ? null : reader.GetString(2), // Handle nullable OriginalTitle
                                IsAdult = reader.GetBoolean(3),
                                StartYear = reader.IsDBNull(4) ? null : reader.GetInt32(4), // Handle nullable StartYear
                                EndYear = reader.IsDBNull(5) ? null : reader.GetInt32(5), // Handle nullable EndYear
                                RuntimeMinutes = reader.IsDBNull(6) ? null : reader.GetInt32(6), // Handle nullable RunTimeMinutes
                                TitleType = reader.GetString(7)
                            });
                        }
                    }
                }
            }

            return titles;
        }

        public List<Name> SearchName(string searchTerm)
        {
            var names = new List<Name>();

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("NameSearch", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@searchTerm", searchTerm);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            names.Add(new Name
                            {
                                NConst = reader.GetString(0),
                                PrimaryName = reader.GetString(1),
                                BirthYear = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                                DeathYear = reader.IsDBNull(3) ? null : reader.GetInt32(3)
                            });
                        }
                    }
                }
            }

            return names;
        }

        public void AddTitle(Title title)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("AddTitle", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@TitleTypeID", title.TitleTypeID);
                    command.Parameters.AddWithValue("@PrimaryTitle", title.PrimaryTitle);
                    command.Parameters.AddWithValue("@OriginalTitle", title.OriginalTitle);
                    command.Parameters.AddWithValue("@IsAdult", title.IsAdult);
                    command.Parameters.AddWithValue("@StartYear", title.StartYear);
                    command.Parameters.AddWithValue("@EndYear", title.EndYear);
                    command.Parameters.AddWithValue("@RunTimeMinutes", title.RuntimeMinutes);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AddName(Name name)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("AddName", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@PrimaryName", name.PrimaryName);
                    command.Parameters.AddWithValue("@BirthYear", name.BirthYear);
                    command.Parameters.AddWithValue("@DeathYear", name.DeathYear);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void UpdateTitle(Title title)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("UpdateTitle", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@Tconst", title.TConst);
                    command.Parameters.AddWithValue("@TitleTypeID", title.TitleTypeID);
                    command.Parameters.AddWithValue("@PrimaryTitle", title.PrimaryTitle);
                    command.Parameters.AddWithValue("@OriginalTitle", title.OriginalTitle);
                    command.Parameters.AddWithValue("@IsAdult", title.IsAdult);
                    command.Parameters.AddWithValue("@StartYear", title.StartYear);
                    command.Parameters.AddWithValue("@EndYear", title.EndYear);
                    command.Parameters.AddWithValue("@RunTimeMinutes", title.RuntimeMinutes);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void DeleteTitle(string tconst)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("DeleteTitle", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@Tconst", tconst);
                    command.ExecuteNonQuery();
                }
            }
        }

    }
}
