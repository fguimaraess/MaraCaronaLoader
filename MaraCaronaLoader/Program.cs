using System;
using System.Threading.Tasks;
using HtmlAgilityPack;
using System.Net.Http;
using System.Linq;
using CrawlerGE.Model;
using Npgsql;
using Newtonsoft.Json;
using System.Collections;
using System.Collections.Generic;

namespace CrawlerGE
{
    class Program
    {
        private static readonly string _connectionString = @"Server=ec2-35-174-88-65.compute-1.amazonaws.com;Port=5432;
                    User Id=odwvjyxqhzgabw;Password=ef7d9e8b0476a76423666b960558df83f905c75c2ca3e2321b7b52d96f9a63b9;Database=d85ko4e25vi0j6;
                    SSL Mode=Require;Trust Server Certificate=true";

        static void Main(string[] args)
        {
            CargaAsync();
        }

        private static async Task CargaAsync()
        {
            try
            {
                var partida = new PartidaDto();

                using (var dbConnection = new NpgsqlConnection(_connectionString))
               {
                    dbConnection.Open();
                    CreateTables(dbConnection);
                    

                    MontaEstrutura(partida, dbConnection);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw e; 
            }
            
        }

        private static void CreateTables(NpgsqlConnection dbConnection)
        {
            var cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS Fixture (
                                                    Id INT GENERATED ALWAYS AS IDENTITY,
                                                    TeamHomeId int NOT NULL,
                                                    TeamAwayId int NOT NULL,
                                                    Location VARCHAR,
                                                    CompetitionId INT NOT NULL,
                                                    Date timestamp NOT NULL,
                                                    PRIMARY KEY (Id)
                                                );

                                                CREATE TABLE IF NOT EXISTS Competition (
                                                    Id INT GENERATED ALWAYS AS IDENTITY,
                                                    Name VARCHAR,
                                                    PRIMARY KEY (Id)
                                                );

                                                CREATE TABLE IF NOT EXISTS Club (
                                                    Id INT GENERATED ALWAYS AS IDENTITY,
                                                    Name VARCHAR NOT NULL,
                                                    PRIMARY KEY (Id)
                                                );", dbConnection);

            cmd.ExecuteNonQuery();
        }

        private static void MontaEstrutura(PartidaDto partida, NpgsqlConnection dbConnection)
        {
            var partidas = JsonConvert.DeserializeObject<IEnumerable<PartidaDto>>(partida.seriea);

            //Criar lógica pra ver se já existe
            //var competitions = partidas.GroupBy(x => x.Competition.Name);
            //competitions.ToList().AsParallel().ForAll(c => 
            //{
            //    var cmd = new NpgsqlCommand($"INSERT INTO Competition (Name) VALUES ('{c.Key}')", dbConnection);
            //    cmd.ExecuteNonQuery();
            //});

            //var clubesHome = partidas.GroupBy(x => x.Home_Name);
            //var clubesAway = partidas.GroupBy(x => x.Away_Name);

            //var clubesString = clubesHome.Union(clubesAway).Select(x => x.Key).Distinct();
            //List<string> insertClube = new List<string>();
            //clubesString.ToList().ForEach(clube =>
            //{
            //    var cmd = new NpgsqlCommand($"INSERT INTO Club (Name) VALUES ('{clube}')", dbConnection);
            //    cmd.ExecuteNonQuery();
            //});


            var clubes = GetInsertedClubs(dbConnection);

            List<Partida> listaDePartidas = new List<Partida>();
            partidas.AsParallel().ForAll(p =>
            {
                listaDePartidas.Add(new Partida
                {
                    CompetitionId = 1,
                    Date = p.Date,
                    Location = p.Location,
                    Team_Away_Id = GetTeamByName(p.Away_Name, clubes).Id,
                    Team_Home_Id = GetTeamByName(p.Home_Name, clubes).Id
                });
            });

            listaDePartidas.ForEach(p =>
            {
                var cmd = new NpgsqlCommand($"SET datestyle = dmy;  INSERT INTO Fixture (TeamHomeId, TeamAwayId, Location, CompetitionId, Date) VALUES ({p.Team_Home_Id}, {p.Team_Away_Id}, '{p.Location}', {p.CompetitionId}, '{p.Date}')", dbConnection);
                cmd.ExecuteNonQuery();
            });
        }

        private static new List<Clube> GetInsertedClubs(NpgsqlConnection dbConnection)
        {
            var cmd = new NpgsqlCommand(@"SELECT * FROM Club", dbConnection);
            var reader = cmd.ExecuteReader();

            var clubes = new List<Clube>();
            while (reader.Read())
            {
                clubes.Add(new Clube
                {
                    Id = Convert.ToInt32(reader["Id"]),
                    Name = reader["Name"].ToString()
                });
            }

            reader.Close();

            return clubes;
        }

        private static Clube GetTeamByName(string name, List<Clube> clubes)
        {
            return clubes?.Where(x => x.Name == name).FirstOrDefault();
        }
    }
}
