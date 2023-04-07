using IMDBConsoleApp.DataProcessor;
using IMDBConsoleApp.DataInserters;
using System.Data.SqlClient;

string titleDataFilePath = @"C:\IMDBdata\title.basics.tsv\data.tsv";
string nameDataFilePath = @"C:\IMDBdata\name.basics.tsv\data.tsv";
string titleCrewDataFilePath = @"C:\IMDBdata\title.crew.tsv\data.tsv";
string connectionString = "server=localhost\\MSSQLSERVER1;database=IMDBdb;User ID=cem;password=Turan!234";

var titleImporter = new IMDBTitleDataBunkInserter(connectionString);
titleImporter.ImportData(titleDataFilePath);
Console.WriteLine("Title Data import completed.");

var nameImporter = new IMDBNameDataBunkInserter(connectionString);
nameImporter.ImportData(nameDataFilePath);
Console.WriteLine("Name Data import completed.");

var titleCrewImporter = new IMDBTitleCrewDataBunkInserter(connectionString);
titleCrewImporter.ImportData(titleCrewDataFilePath);
Console.WriteLine("TitleCrew Data import completed.");

