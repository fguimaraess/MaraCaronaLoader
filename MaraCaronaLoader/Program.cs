﻿using CrawlerGE.Model;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;

namespace CrawlerGE
{
    class Program
    {
        private static readonly string _connectionString = @"Server=ec2-35-174-88-65.compute-1.amazonaws.com;Port=5432;" +
                    "User Id=odwvjyxqhzgabw;Password=ef7d9e8b0476a76423666b960558df83f905c75c2ca3e2321b7b52d96f9a63b9;Database=d85ko4e25vi0j6";
        static void Main(string[] args)
        {
            CargaAsync();
        }



        private static async Task CargaAsync()
        {

            try
            {

                int page = 1;
                List<string> resposta = new List<string>();

                while (page <= 13)
                {
                    string baseUrl = "https://live-score-api.com/api-client/fixtures/matches.json?&key=jxKfM3GpOje11Jbl&secret=gW6tfW4qP72wTiycQirDm72argcsZnOg&competition_id=24&page=" + page;
                    var client = new RestClient(baseUrl);
                    var request = new RestRequest(Method.GET);

                    IRestResponse response = client.Execute(request);
                    resposta.Add(response.Content);
                    page++;
                }
                resposta.ForEach(model =>
                {
                    using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
                    {
                        conn.Open();
                        var cmd = new NpgsqlCommand($"INSERT INTO CLUBE (Name) VALUES ({clube})");
                    }

                })
                var partida = new PartidaDto();

                using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
                {
                    conn.Open();
                    var cmd = new NpgsqlCommand(@"CREATE TABLE PARTIDA (
                                                    Id INT GENERATED ALWAYS AS IDENTITY,
                                                    username VARCHAR NOT NULL,
                                                    password VARCHAR NOT NULL,
                                                    email VARCHAR,
                                                    Competition VARCHAR,
                                                    Date VARCHAR NOT NULL
                                                );
                                                CREATE TABLE CLUBE(
                                                    Id INT GENERATED ALWAYS AS IDENTITY,
                                                    Name int NOT NULL
                                                ); ", conn);

                    var partida = new PartidaDto();
                    var partidas = JsonConvert.DeserializeObject<IEnumerable<PartidaDto>>(partida.seriea);
                    MontaEstrutura(partidas, dbConnection);

                    partidas = JsonConvert.DeserializeObject<IEnumerable<PartidaDto>>(partida.serieb);
                    MontaEstrutura(partidas, dbConnection);
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
                                                    FixtureRound INT NOT NULL,
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

        private static void MontaEstrutura(IEnumerable<PartidaDto> partidas, NpgsqlConnection dbConnection)
        {
            //Criar lógica pra ver se já existe
            var competitions = partidas.GroupBy(x => x.Competition.Name);
            competitions.ToList().ForEach(c =>
            {
                var cmd = new NpgsqlCommand($"INSERT INTO Competition (Name) VALUES ('{c.Key}')", dbConnection);
                cmd.ExecuteNonQuery();
            });

            var clubesHome = partidas.GroupBy(x => x.Home_Name);
            var clubesAway = partidas.GroupBy(x => x.Away_Name);

            var clubesString = clubesHome.Union(clubesAway).Select(x => x.Key).Distinct();
            List<string> insertClube = new List<string>();
            clubesString.ToList().ForEach(clube =>
            {
                var cmd = new NpgsqlCommand($"INSERT INTO Club (Name) VALUES ('{clube}')", dbConnection);
                cmd.ExecuteNonQuery();
            });


            var clubes = GetInsertedClubs(dbConnection);
            var competitionsInserted = GetInsertedCompetitions(dbConnection);

            List<Partida> listaDePartidas = new List<Partida>();
            partidas.AsParallel().ForAll(p =>
            {
                listaDePartidas.Add(new Partida
                {
                    CompetitionId = GetCompetitionByName(p.Competition.Name, competitionsInserted).Id,
                    Date = p.Date.Date.Add(p.Time.TimeOfDay),
                    Location = p.Location,
                    Team_Away_Id = GetTeamByName(p.Away_Name, clubes).Id,
                    Team_Home_Id = GetTeamByName(p.Home_Name, clubes).Id,
                    FixtureRound = p.Round
                });
            });

            listaDePartidas.ForEach(p =>
            {
                var cmd = new NpgsqlCommand($@"SET datestyle = dmy;  INSERT INTO Fixture (TeamHomeId, TeamAwayId, Location, CompetitionId, FixtureRound, Date) 
                                            VALUES ({p.Team_Home_Id}, {p.Team_Away_Id}, '{p.Location}', {p.CompetitionId}, {p.FixtureRound}, '{p.Date}')", dbConnection);
                cmd.ExecuteNonQuery();
            });
        }

        private static List<Competition> GetInsertedCompetitions(NpgsqlConnection dbConnection)
        {
            var cmd = new NpgsqlCommand(@"SELECT * FROM Competition", dbConnection);
            var reader = cmd.ExecuteReader();

            var competitions = new List<Competition>();
            while (reader.Read())
            {
                competitions.Add(new Competition
                {
                    Id = Convert.ToInt32(reader["Id"]),
                    Name = reader["Name"].ToString()
                });
            }

            reader.Close();

            return competitions;
        }

        private static Competition GetCompetitionByName(string name, List<Competition> competitionsInserted)
        {
            return competitionsInserted?.FirstOrDefault(x => x.Name == name);
        }

        private static List<Clube> GetInsertedClubs(NpgsqlConnection dbConnection)
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
