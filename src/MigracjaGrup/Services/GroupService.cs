using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using cdn_api;
using Dapper;
using ENSOFT_Utils;
using MigracjaGrup.Models;
using Newtonsoft.Json;

namespace MigracjaGrup
{
    public class GroupService
    {
        private List<Row> _rows { get; set; } = new List<Row>();
        private int GrupaSklep { get; set; } = 37003; 
        private string GrupaSklepName { get; set; } = "ESKLEP_2023";
        private int? GidPoziom1 { get; set; }
        private int? GidPoziom2 { get; set; }
        private int? GidPoziom3 { get; set; }
        private int? GidPoziom4 { get; set; }
        private static int xlSesja = -99;
        private static int xlDok = -88;
        public GroupService()
        {
            string jsonData = File.ReadAllText("grupy_twr.json");
            
            _rows = JsonConvert.DeserializeObject<List<Row>>(jsonData);
        }

        public void Migruj()
        {
            if (_rows == null)
            {
                Tools.WriteLog("Lista json jest pusta!", true, true, ConsoleColor.Red);
                return;
            }

            var loginScore = cdn_api.cdn_api.XLLogin(new XLLoginInfo_20231()
            {
                Wersja = AppSettings.XL.Wersja,
                UtworzWlasnaSesje = 0,
                Winieta = -1,
                TrybWsadowy = 1,
                TrybNaprawy = 1,
                ProgramID = "ProdukcjaAutomat",
                OpeIdent = AppSettings.XL.Login,
                OpeHaslo = AppSettings.XL.Pass,
                Baza = AppSettings.XL.Baza
            }, ref xlSesja);

            if (loginScore != 0)
            {
                Tools.WriteLog($"Błąd logowania XL: {loginScore}", true, true, ConsoleColor.Red);
                return;
            }

            int counter = 0;
            
            foreach (var row in _rows)
            {
                counter++;
                
                if (counter % 500 == 0)
                {
                    Tools.WriteLog("Wykonano: " + counter, true,true,ConsoleColor.Yellow);
                }
                
                if (row.Kod.ToUpper() == row.Poziom1.ToUpper() && row.Poziom2=="" && row.Poziom3=="" && row.Poziom4=="")
                {
                    GidPoziom1 = null;
                    GidPoziom2 = null;
                    GidPoziom3 = null;
                    GidPoziom4 = null;
                    
                    
                    
                    var podgrupy = PobierzPodgrupy(GrupaSklep);

                    if (podgrupy.Any(x => x.Nazwa == row.Kod.ToUpper()))
                    {
                        Tools.WriteLog($"Podgrupa o nazwie {row.Kod} już istnieje w grupie {GrupaSklepName}", true, true, ConsoleColor.Yellow);

                        GidPoziom1 = podgrupy.FirstOrDefault(x => x.Nazwa == row.Kod.ToUpper()).Numer;
                        
                        continue;
                    }

                    var dodajGrupeScore = DodajGrupe(row.Kod, row.Poziom1, GrupaSklep);

                    if (dodajGrupeScore == -1)
                    {
                        Tools.WriteLog($"Błąd dodawnia grupy dla {row.Kod}");
                        continue;
                    }
                    
                    GidPoziom1 = dodajGrupeScore;
                    
                    continue;
                }

                if (row.Kod.ToUpper() != row.Poziom1.ToUpper() && row.Poziom2 != "" && row.Poziom3 == "" && row.Poziom4 == "" )
                {
                    var podgrupy = PobierzPodgrupy((int)GidPoziom1);
                    
                    if (podgrupy.Any(x => x.Nazwa == row.Kod.ToUpper()))
                    {
                        Tools.WriteLog($"Podgrupa o nazwie {row.Kod} już istnieje w grupie {(int)GidPoziom1}", true, true, ConsoleColor.Yellow);

                        GidPoziom2 = podgrupy.FirstOrDefault(x => x.Nazwa == row.Kod.ToUpper()).Numer;
                        
                        continue;
                    }

                    var grupaNadrzedna = PobierzNumerGrupyNadrzednej_Poziom1(row.Poziom1);
                    if (grupaNadrzedna == 0)
                    {
                        Tools.WriteLog($"Błąd odnalezienie grupy nadrzędnej dla {row.Kod}");
                        continue;
                    }
                    var dodajGrupeScore = DodajGrupe(row.Kod, row.Poziom2, grupaNadrzedna);

                    if (dodajGrupeScore == -1)
                    {
                        Tools.WriteLog($"Błąd dodawnia grupy dla {row.Kod}");
                        continue;
                    }
                    
                    GidPoziom2 = dodajGrupeScore;
                    
                    continue;
                }

                if (row.Kod.ToUpper() != row.Poziom1.ToUpper() && row.Poziom2 != "" && row.Poziom3 != "" && row.Poziom4 == "")
                {
                    var podgrupy = PobierzPodgrupy((int)GidPoziom2);
                    
                    if (podgrupy.Any(x => x.Nazwa == row.Kod.ToUpper()))
                    {
                        Tools.WriteLog($"Podgrupa o nazwie {row.Kod} już istnieje w grupie {(int)GidPoziom2}", true, true, ConsoleColor.Yellow);

                        GidPoziom3 = podgrupy.FirstOrDefault(x => x.Nazwa == row.Kod.ToUpper()).Numer;
                        
                        continue;
                    }
                    
                    var grupaNadrzedna = PobierzNumerGrupyNadrzednej_Poziom2(row.Poziom1, row.Poziom2);
                    if (grupaNadrzedna == 0)
                    {
                        Tools.WriteLog($"Błąd odnalezienie grupy nadrzędnej dla {row.Kod}");
                        continue;
                    }
                    var dodajGrupeScore = DodajGrupe(row.Kod, row.Poziom3, grupaNadrzedna);

                    if (dodajGrupeScore == -1)
                    {
                        Tools.WriteLog($"Błąd dodawnia grupy dla {row.Kod}");
                        continue;
                    }

                    GidPoziom3 = dodajGrupeScore;
                    
                    continue;
                }
                
                if (row.Kod.ToUpper() != row.Poziom1.ToUpper() && row.Poziom2 != "" && row.Poziom3 != "" && row.Poziom4 != "")
                {
                    var podgrupy = PobierzPodgrupy((int)GidPoziom3);
                    
                    if (podgrupy.Any(x => x.Nazwa == row.Kod.ToUpper()))
                    {
                        Tools.WriteLog($"Podgrupa o nazwie {row.Kod} już istnieje w grupie {(int)GidPoziom3}", true, true, ConsoleColor.Yellow);

                        GidPoziom4 = podgrupy.FirstOrDefault(x => x.Nazwa == row.Kod.ToUpper()).Numer;
                        
                        continue;
                    }
                    
                    var grupaNadrzedna = PobierzNumerGrupyNadrzednej_Poziom3(row.Poziom1, row.Poziom2, row.Poziom3);
                    if (grupaNadrzedna == 0)
                    {
                        Tools.WriteLog($"Błąd odnalezienie grupy nadrzędnej dla {row.Kod}");
                        continue;
                    }
                    var dodajGrupeScore = DodajGrupe(row.Kod, row.Poziom4, grupaNadrzedna);

                    if (dodajGrupeScore == -1)
                    {
                        Tools.WriteLog($"Błąd dodawnia grupy dla {row.Kod}");
                        continue;
                    }

                    GidPoziom4 = dodajGrupeScore;
                    
                    continue;
                }
                
                
            }
            
            cdn_api.cdn_api.XLLogout(xlSesja);
            
        }

        private List<Grupy> PobierzPodgrupy(int grupa)
        {
            return AppSettings.SQL.SqlConnection.Query<Grupy>($"declare @GroNumer int; set @GroNumer = {grupa}" + @"

                                            SELECT  A.TwG_Kod as Nazwa, A.Twg_Gidnumer as Numer

                                            FROM  {oj CDN.TwrGrupy A LEFT OUTER JOIN CDN.TwrKarty  
                                            ON  A.TwG_GIDTyp = 16 AND  A.TwG_GIDNumer = Twr_GIDNumer 
                                            LEFT OUTER JOIN CDN.TwrJm C ON 1=0 
                                            LEFT OUTER JOIN CDN.TwrCeny D ON D.TwC_TwrNumer = Twr_GIDNumer 
                                            AND D.TwC_TwrLp = 0 and D.TwC_DataOd <=1047041445 
                                            LEFT OUTER JOIN CDN.TwrCeny E 
                                            ON Twr_GIDNumer = e.TwC_TwrNumer AND Twr_CenaSpr = e.TwC_TwrLp and 
                                            e.TwC_DataOd <=1047041445 LEFT OUTER JOIN CDN.Techniczna F ON 1 = 0 } 
                                            WHERE ( ( TwG_GrOTyp=-16 AND TwG_GrONumer=@GroNumer  AND (TwG_GIDTyp=16 OR 1=1)) 
                                            AND (Twr_Typ<>4 OR TwG_GIDTyp=-16) AND (Twr_Typ<>3 OR TwG_GIDTyp=-16) AND (Twr_Archiwalny=0 OR TwG_GIDTyp=-16) 
                                            and ( e.TwC_Id in (SELECT zzz.TwC_Id FROM CDN.ZwrocCeneAktualna(Twr_GIDNumer, Twr_CenaSpr, 1047041445) zzz) 
                                            OR E.TwC_DataOd IS NULL) and ( d.TwC_Id in (SELECT zzz.TwC_Id FROM CDN.ZwrocCeneAktualna(Twr_GIDNumer, 0, 1047041445) zzz) 
                                            OR D.TwC_DataOd IS NULL) )  ORDER BY  A.TwG_GIDTyp,  A.TwG_Kod,  A.TwG_GrONumer").ToList();
        }

        private int DodajGrupe(string kod, string nazwa, int grupaNadrzedna)
        {
            try
            {
                XLGrupaTwrInfo_20231 grupaInfo = new XLGrupaTwrInfo_20231()
                {
                    Wersja = AppSettings.XL.Wersja,
                    Kod = kod.ToUpper(),
                    Nazwa = nazwa,
                    GrONumer = grupaNadrzedna
                };

                var nowaGrupaScore = cdn_api.cdn_api.XLNowaGrupaTwr(xlSesja, grupaInfo);

                if (nowaGrupaScore != 0)
                    return -1;

                return grupaInfo.GIDNumer;
            }
            catch (Exception e)
            {
                Tools.WriteLog(e.Message, true, true, ConsoleColor.Red);
                return -1;
            }
        }
        
        private int PobierzNumerGrupyNadrzednej_Poziom1(string poziom1)
        {
            return AppSettings.SQL.SqlConnection.Query<int>($@"declare @Poziom0Nazwa varchar(max), @Poziom1Nazwa varchar(max), @Poziom2Nazwa varchar(max), 
            @Poziom3Nazwa varchar(max), @Poziom4Nazwa varchar(max);
            declare @Poziom0Kod varchar(max), @Poziom1Kod varchar(max), @Poziom2Kod varchar(max), @Poziom3Kod varchar(max), @Poziom4Kod varchar(max);
            declare @Poziom0GroNumer int, @Poziom1GroNumer int, @Poziom2GroNumer int, @Poziom3GroNumer int, @Poziom4GroNumer int;

            set @Poziom0Nazwa = '{GrupaSklepName}';

            select @Poziom0Kod=TGD_Kod, @Poziom0GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
            where TwG_Nazwa=@Poziom0Nazwa

            set @Poziom1Nazwa = '{poziom1}';

            select TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
            where TwG_Nazwa=@Poziom1Nazwa and TGD_GrONumer=@Poziom0GroNumer").FirstOrDefault();
        }
        
        private int PobierzNumerGrupyNadrzednej_Poziom2(string poziom1, string poziom2)
        {
            return AppSettings.SQL.SqlConnection.Query<int>($@"declare @Poziom0Nazwa varchar(max), @Poziom1Nazwa varchar(max), @Poziom2Nazwa varchar(max), @Poziom3Nazwa varchar(max), @Poziom4Nazwa varchar(max);
                declare @Poziom0Kod varchar(max), @Poziom1Kod varchar(max), @Poziom2Kod varchar(max), @Poziom3Kod varchar(max), @Poziom4Kod varchar(max);
                declare @Poziom0GroNumer int, @Poziom1GroNumer int, @Poziom2GroNumer int, @Poziom3GroNumer int, @Poziom4GroNumer int;

                set @Poziom0Nazwa = '{GrupaSklepName}';

                select @Poziom0Kod=TGD_Kod, @Poziom0GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom0Nazwa

                set @Poziom1Nazwa = '{poziom1}';

                select @Poziom1Kod=TGD_Kod, @Poziom1GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom1Nazwa and TGD_GrONumer=@Poziom0GroNumer

                set @Poziom2Nazwa = '{poziom2}';

                select TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom2Nazwa and TGD_GrONumer=@Poziom1GroNumer").FirstOrDefault();
        }
        
        private int PobierzNumerGrupyNadrzednej_Poziom3(string poziom1, string poziom2, string poziom3)
        {
            return AppSettings.SQL.SqlConnection.Query<int>($@"declare @Poziom0Nazwa varchar(max), @Poziom1Nazwa varchar(max), @Poziom2Nazwa varchar(max), @Poziom3Nazwa varchar(max), @Poziom4Nazwa varchar(max);
                declare @Poziom0Kod varchar(max), @Poziom1Kod varchar(max), @Poziom2Kod varchar(max), @Poziom3Kod varchar(max), @Poziom4Kod varchar(max);
                declare @Poziom0GroNumer int, @Poziom1GroNumer int, @Poziom2GroNumer int, @Poziom3GroNumer int, @Poziom4GroNumer int;

                set @Poziom0Nazwa = '{GrupaSklepName}';

                select @Poziom0Kod=TGD_Kod, @Poziom0GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom0Nazwa

                set @Poziom1Nazwa = '{poziom1}';

                select @Poziom1Kod=TGD_Kod, @Poziom1GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom1Nazwa and TGD_GrONumer=@Poziom0GroNumer

                set @Poziom2Nazwa = '{poziom2}';

                select @Poziom2Kod=TGD_Kod, @Poziom2GroNumer=TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom2Nazwa and TGD_GrONumer=@Poziom1GroNumer

                set @Poziom3Nazwa = '{poziom3}'; 

                select TGD_GIDNumer from cdn.TwrGrupyDom 
                inner join cdn.TwrGrupy on TGD_GIDNumer=TwG_GIDNumer and TGD_GIDTyp=TwG_GIDTyp and TGD_GrONumer=TwG_GrONumer and TGD_GIDTyp=TwG_GrOTyp
                where TwG_Nazwa=@Poziom3Nazwa and TGD_GrONumer=@Poziom2GroNumer").FirstOrDefault();
        }
        
    }
}