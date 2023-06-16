using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using ENSOFT_Utils;
using MigracjaGrup.Models;
using Newtonsoft.Json;

namespace MigracjaGrup
{
    public class TwrService
    {
        private IEnumerable<TwrExcel> _twrRows { get; set; }

        private string[] _grGlowne = new[]
        {
            "AKCESORIA", "CZĘŚCI", "CZĘŚCI NASZYCH MASZYN", "MASZYNY BUDOWLANE", "MASZYNY OGRODNICZE", "NARZĘDZIA",
            "NOWOŚCI", "OFERTA TYGODNIA", "OLEJE I SMARY", "OSPRZĘT DO MASZYN", "PROMOCJE", "SILNIKI", "WYPRZEDAŻ"
        };
        
        private string[] _grGlowne_ang = new[]
        {
            "Accessories", "PARTS", "Parts of our machines", "Construction equipment", "GARDEN MACHINERY", "TOOLS",
            "NEWS", "OFFER OF THE WEEK", "Oils and greases", "ATTACHMENTS FOR MACHINES", "PROMOTIONS", "ENGINES", "SALE"
        };

        private string _nazwaGrGlownej = "ESKLEP_2023";

        public TwrService()
        {
            string jsonDataTwr = File.ReadAllText("towary.json");
            
            _twrRows = JsonConvert.DeserializeObject<IEnumerable<TwrExcel>>(jsonDataTwr);
        }

        public void Migruj()
        {
            int counter = 0;
            
            foreach (var twr in _twrRows)
            {
                if(counter % 1000 == 0) 
                    Tools.WriteLog("Wykonano:" + counter);
                    
                List<string> grupa = new List<string>();

                List<string> paths = new List<string>();
                
                if(twr.Grupa1 != "")
                    grupa.Add(twr.Grupa1);
                
                if(twr.Grupa2 != "")
                    grupa.Add(twr.Grupa2);
                
                if(twr.Grupa3 != "")
                    grupa.Add(twr.Grupa3);
                
                if(twr.Grupa4 != "")
                    grupa.Add(twr.Grupa4);
                
                if(twr.Grupa5 != "")
                    grupa.Add(twr.Grupa5);
                
                if(twr.Grupa6 != "")
                    grupa.Add(twr.Grupa6);
                
                if(twr.Grupa7 != "")
                    grupa.Add(twr.Grupa7);
                
                if(twr.Grupa8 != "")
                    grupa.Add(twr.Grupa8);
                
                if(twr.Grupa9 != "")
                    grupa.Add(twr.Grupa9);
                
                if(twr.Grupa10 != "")
                    grupa.Add(twr.Grupa10);
                
                if(twr.Grupa11 != "")
                    grupa.Add(twr.Grupa11);
                
                if(twr.Grupa12 != "")
                    grupa.Add(twr.Grupa12);
                
                if(twr.Grupa13 != "")
                    grupa.Add(twr.Grupa13);
                
                if(twr.Grupa14 != "")
                    grupa.Add(twr.Grupa14);
                
                if(twr.Grupa15 != "")
                    grupa.Add(twr.Grupa15);

                string path = string.Join("/", grupa).ToUpper();
                
                List<string> paths_ang = new List<string>();
                
                List<string> group_ang = new List<string>();

                if(twr.Group1 != "")
                    group_ang.Add(twr.Group1);
                
                if(twr.Group2 != "")
                    group_ang.Add(twr.Group2);
                
                if(twr.Group3 != "")
                    group_ang.Add(twr.Group3);
                
                if(twr.Group4 != "")
                    group_ang.Add(twr.Group4);
                
                if(twr.Group5 != "")
                    group_ang.Add(twr.Group5);
                
                if(twr.Group6 != "")
                    group_ang.Add(twr.Group6);
                
                if(twr.Group7 != "")
                    group_ang.Add(twr.Group7);
                
                if(twr.Group8 != "")
                    group_ang.Add(twr.Group8);
                
                if(twr.Group9 != "")
                    group_ang.Add(twr.Group9);
                
                if(twr.Group10 != "")
                    group_ang.Add(twr.Group10);
                
                if(twr.Group11 != "")
                    group_ang.Add(twr.Group11);
                
                if(twr.Group12 != "")
                    group_ang.Add(twr.Group12);
                
                if(twr.Group13 != "")
                    group_ang.Add(twr.Group13);
                
                if(twr.Group14 != "")
                    group_ang.Add(twr.Group14);
                
                if(twr.Group15 != "")
                    group_ang.Add(twr.Group15);
                
                string path_ang = string.Join("/", group_ang).ToUpper();
                
                if (path == "")
                    continue;
                
                if(path_ang == "")
                    continue;
                    
                var score = path.ToUpper().Split(_grGlowne, StringSplitOptions.None);
                    
                foreach (var g in _grGlowne)
                {
                    foreach (var s in score.Where(x=>x!="").ToList())
                    {
                        Regex regex = new Regex($@"{g}{s}");

                        var isMatch = regex.IsMatch(path);
                        
                        if(isMatch)
                            paths.Add((g+s).TrimEnd('/'));
                    }
                }
                
                var score_ang = path_ang.ToUpper().Split(_grGlowne_ang, StringSplitOptions.None);
                    
                foreach (var g in _grGlowne_ang)
                {
                    foreach (var s in score_ang.Where(x=>x!="").ToList())
                    {
                        Regex regex = new Regex($@"{g}{s}");

                        var isMatch = regex.IsMatch(path_ang);
                        
                        if(isMatch)
                            paths_ang.Add((g+s).TrimEnd('/'));
                    }
                }

                int i = 0;
                foreach (var p in paths)
                {
                    //TODO: info, że nie odnalazł grupy lub twr
                    
                    var tgd = PobierzTwrGrupyDomByPath(p);
                    
                    if(tgd == null)
                        continue;

                    var twrKarta = PobierzTwr(twr.Kod);
                    
                    if(twrKarta == null)
                        continue;

                    try
                    {
                        DodajTowarDoGrupy(tgd, twrKarta);
                        Tools.WriteLog($"Poprawnie dodano towar {twr.Kod} do grupy {p}");
                    }
                    catch (Exception e)
                    {
                        Tools.WriteLog($"Błąd podczas dodawnia towaru {twr.Kod} do grupy {p}");
                    }

                    try
                    {
                        DodajTlumaczenie(tgd, paths_ang[i].Split('/').Last());
                    }
                    catch (Exception e)
                    {
                        Tools.WriteLog($"Błąd podczas dodawnia tłumaczenia do grupy {p}");
                    }

                    i++;
                }

                counter++;
            }
        }

        private void DodajTlumaczenie(TwrGrupyDom tgd, string tlumaczenieGrupy)
        {
            AppSettings.SQL.SqlConnection.Execute($@"declare @Tlumaczenie varchar(max); set @Tlumaczenie = '{tlumaczenieGrupy}';
                                                    declare @GrNumer int; set @GrNumer = {tgd.TGD_GIDNumer};

                                                    if not exists (select * from cdn.Tlumaczenia where TLM_Typ=-16 and TLM_Numer=@GrNumer and TLM_Lp=0 and TLM_Jezyk=701 and TLM_Pole=1)
                                                    AND not exists (select * from cdn.Tlumaczenia where TLM_Typ=-16 and TLM_Numer=@GrNumer and TLM_Lp=0 and TLM_Jezyk=701 and TLM_Pole=2)
                                                    BEGIN 

	                                                    insert into cdn.Tlumaczenia VALUES (-16, @GrNumer, 0, 1, 701, @Tlumaczenie)
	                                                    insert into cdn.Tlumaczenia VALUES (-16, @GrNumer, 0, 2, 701, @Tlumaczenie)

                                                    END");
        }

        private TwrGrupyDom PobierzTwrGrupyDomByPath(string path)
        {
            return AppSettings.SQL.SqlConnection.Query<TwrGrupyDom>($"select * from cdn.TwrGrupyDom where cdn.TwrGrupaPelnaNazwa(TGD_GIDNumer)='{_nazwaGrGlownej}/{path}'").FirstOrDefault();
        }

        private Twr PobierzTwr(string kod)
        {
            return AppSettings.SQL.SqlConnection.Query<Twr>($@"select 
                                                            Twr_GIDNumer as Numer, 
                                                            Twr_GIDTyp as Typ,
                                                            Twr_GIDFirma as Firma,
                                                            Twr_GIDLp as Lp,
                                                            Twr_Kod as Kod,
                                                            Twr_Nazwa as Nazwa
                                                            from cdn.TwrKarty where twr_kod='{kod}'").FirstOrDefault();
        }

        private void DodajTowarDoGrupy(TwrGrupyDom tgd, Twr twr)
        {
            //TODO: sprawdzenie czy udalo sie dodac
            
            AppSettings.SQL.SqlConnection.Execute($@"
                            INSERT INTO CDN.TwrGrupy (TwG_GIDTyp,TwG_GIDFirma,TwG_GIDNumer,TwG_GIDLp,TwG_GrOTyp,TwG_GrOFirma,TwG_GrONumer,TwG_GrOLp,TwG_Kod,TwG_Nazwa,TwG_CzasModyfikacji,TwG_KategoriaBIId) 

                            VALUES (
                                    {twr.Typ},
                                    {twr.Firma},
                                    {twr.Numer},
                                    {twr.Lp},
                                    {tgd.TGD_GIDTyp},
                                    {tgd.TGD_GrOFirma},
                                    {tgd.TGD_GIDNumer},
                                    {tgd.TGD_GIDLp},
                                    '{twr.Kod}',
                                    '{twr.Nazwa}',
                                    {Tools.DateToTS(DateTime.Now)},
                                    0
                                    ); 
");
        }
        
    }
}
