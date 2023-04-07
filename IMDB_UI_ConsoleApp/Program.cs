using IMDB_UI_ConsoleApp.Services;
using IMDBLib;

string connectionString = "server=localhost\\MSSQLSERVER1;database=IMDBdb;User ID=IMDBuser;password=secret";
IMDBService imdbService = new IMDBService(connectionString);

while (true)
{
    Console.WriteLine("Please choose an option:");
    Console.WriteLine("1. Search for a Title");
    Console.WriteLine("2. Search for a Name");
    Console.WriteLine("3. Add a Title");
    Console.WriteLine("4. Add a Name");
    Console.WriteLine("5. Update Title");
    Console.WriteLine("6. Delete Title");
    Console.WriteLine("0. Exit");
    Console.Write("Enter your choice: ");
    string choice = Console.ReadLine();

    switch (choice)
    {
        case "1":
            Console.Write("Enter search term for Title: ");
            string titleSearchTerm = Console.ReadLine();
            var titles = imdbService.SearchTitle(titleSearchTerm);
            DisplayTitles(titles);
            break;
        case "2":
            Console.Write("Enter search term for Name: ");
            string nameSearchTerm = Console.ReadLine();
            var names = imdbService.SearchName(nameSearchTerm);
            DisplayNames(names);
            break;
        case "3":
            AddTitle(imdbService);
            break;
        case "4":
            AddName(imdbService);
            break;
        case "5":
            UpdateTitle(imdbService);
            break;
        case "6":
            DeleteTitle(imdbService);
            break;
        case "0":
            return;
        default:
            Console.WriteLine("Invalid choice. Please try again.");
            break;
    }
}

static void DisplayTitles(List<Title> titles)
{
    if (titles.Count > 0)
    {
        Console.ForegroundColor = ConsoleColor.DarkGreen;
        Console.WriteLine("\nResults:");
        foreach (var title in titles)
        {
            Console.WriteLine($"Tconst: {title.TConst}\n" +
                              $"PrimaryTitle: {title.PrimaryTitle}\n" +
                              $"OriginalTitle: {title.OriginalTitle}\n" +
                              $"IsAdult: {title.IsAdult}\n" +
                              $"StartYear: {title.StartYear}\n" +
                              $"EndYear: {title.EndYear}\n" +
                              $"RunTimeMinutes: {title.RuntimeMinutes}\n" +
                              $"TitleType: {title.TitleType}\n" +
                              $"=================");
        }
    }
    else
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("No results found.");
    }
    Console.ForegroundColor = ConsoleColor.White;
}

static void DisplayNames(List<Name> names)
{
    if (names.Count > 0)
    {
        Console.ForegroundColor = ConsoleColor.DarkGreen;
        Console.WriteLine("\nResults:");
        foreach (var name in names)
        {
            Console.WriteLine($"Nconst: {name.NConst}\n" +
                              $"PrimaryName: {name.PrimaryName}\n" +
                              $"BirthYear: {name.BirthYear}\n" +
                              $"DeathYear: {name.DeathYear}\n" +
                              $"=================");
        }
    }
    else
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("No results found.");
    }
    Console.ForegroundColor = ConsoleColor.White;
}

static void AddTitle(IMDBService imdbService)
{
    Title title = new Title();
    Console.Write("Enter TitleTypeID: ");
    title.TitleTypeID = int.Parse(Console.ReadLine());
    Console.Write("Enter PrimaryTitle: ");
    title.PrimaryTitle = Console.ReadLine();
    Console.Write("Enter OriginalTitle: ");
    title.OriginalTitle = Console.ReadLine();
    Console.Write("Enter IsAdult (0 or 1): ");
    title.IsAdult = int.Parse(Console.ReadLine()) == 1;
    Console.Write("Enter StartYear: ");
    title.StartYear = int.Parse(Console.ReadLine());
    Console.Write("Enter EndYear: ");
    title.EndYear = int.Parse(Console.ReadLine());
    Console.Write("Enter RunTimeMinutes: ");
    title.RuntimeMinutes = int.Parse(Console.ReadLine());

    imdbService.AddTitle(title);

    Console.ForegroundColor = ConsoleColor.DarkGreen;
    Console.WriteLine("Title added successfully.");
    Console.ForegroundColor = ConsoleColor.White;
}

static void AddName(IMDBService imdbService)
{
    Name name = new Name();
    Console.Write("Enter PrimaryName: ");
    name.PrimaryName = Console.ReadLine();
    Console.Write("Enter BirthYear: ");
    name.BirthYear = int.Parse(Console.ReadLine());
    Console.Write("Enter DeathYear: ");
    name.DeathYear = int.Parse(Console.ReadLine());
    imdbService.AddName(name);

    Console.ForegroundColor = ConsoleColor.DarkGreen;
    Console.WriteLine("Name added successfully.");
    Console.ForegroundColor = ConsoleColor.White;
}

static void UpdateTitle(IMDBService imdbService)
{
    Console.Write("Enter Tconst of the title to update: ");
    string tconst = Console.ReadLine();
    Title title = new Title
    {
        TConst = tconst
    };
    Console.Write("Enter new TitleTypeID: ");
    title.TitleTypeID = int.Parse(Console.ReadLine());
    Console.Write("Enter new PrimaryTitle: ");
    title.PrimaryTitle = Console.ReadLine();
    Console.Write("Enter new OriginalTitle: ");
    title.OriginalTitle = Console.ReadLine();
    Console.Write("Enter new IsAdult (0 or 1): ");
    title.IsAdult = int.Parse(Console.ReadLine()) == 1;
    Console.Write("Enter new StartYear: ");
    title.StartYear = int.Parse(Console.ReadLine());
    Console.Write("Enter new EndYear: ");
    title.EndYear = int.Parse(Console.ReadLine());
    Console.Write("Enter new RunTimeMinutes: ");
    title.RuntimeMinutes = int.Parse(Console.ReadLine());
    imdbService.UpdateTitle(title);

    Console.ForegroundColor = ConsoleColor.DarkGreen;
    Console.WriteLine("Title updated successfully.");
    Console.ForegroundColor = ConsoleColor.White;
}

static void DeleteTitle(IMDBService imdbService)
{
    Console.Write("Enter Tconst of the title to delete: ");
    string tconst = Console.ReadLine();
    imdbService.DeleteTitle(tconst);
    Console.ForegroundColor = ConsoleColor.DarkGreen;
    Console.WriteLine("Title deleted successfully.");
    Console.ForegroundColor = ConsoleColor.White;
}