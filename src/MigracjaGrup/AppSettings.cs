using System.Data.SqlClient;

namespace MigracjaGrup
{
    public class AppSettings
    {
        public static void Load()
        {
            SQL.Server = "X";
            SQL.Baza = "X";
            SQL.Login = "X";
            SQL.Pass = "X";
            
            SQL.ConnString = $"DATA SOURCE={SQL.Server};INITIAL CATALOG={SQL.Baza};User Id={SQL.Login};Password={SQL.Pass};";

            SQL.SqlConnection = new SqlConnection(SQL.ConnString);

            XL.Wersja = 20231;
            XL.Login = "y";
            XL.Pass = "y";
            XL.Baza = "y";
        }
        
        

        public class SQL
        {
            public static string ConnString { get; set; }
            public static SqlConnection SqlConnection { get; set; }
            public static string Server { get; set; }
            public static string Baza { get; set; }
            public static string Login { get; set; }
            public static string Pass { get; set; }
        }

        public class XL
        {
            public static int Wersja { get; set; }
            public static string Login { get; set; }
            public static string Pass { get; set; }
            public static string Baza { get; set; }
        }
    }
}