using System;
using ENSOFT_Utils;

namespace MigracjaGrup
{
    internal class Program
    {
        public static void Main()
        {
            AppSettings.Load();
            
            try
            {
                //GroupService groupService = new GroupService();
                //groupService.Migruj();
     
                TwrService twrService = new TwrService();
                twrService.Migruj();
            }
            catch (Exception e)
            {
                Tools.WriteLog(e.ToString(), true, true, ConsoleColor.Red);
            }
            
            
            Tools.WriteLog("Koniec!", true, true, ConsoleColor.Green);
        }
    }
}