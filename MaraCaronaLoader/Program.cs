using CrawlerGE.Model;
using Newtonsoft.Json;
using Npgsql;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using System.Threading.Tasks;
using MaraCaronaLoader.Model;
using NpgsqlTypes;

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

                //int page = 1;
                //List<string> resposta = new List<string>();

                //while (page <= 13)
                //{
                //    string baseUrl = "https://live-score-api.com/api-client/fixtures/matches.json?&key=jxKfM3GpOje11Jbl&secret=gW6tfW4qP72wTiycQirDm72argcsZnOg&competition_id=24&page=" + page;
                //    var client = new RestClient(baseUrl);
                //    var request = new RestRequest(Method.GET);

                //    IRestResponse response = client.Execute(request);
                //    resposta.Add(response.Content);
                //    page++;
                //}
                //resposta.ForEach(model =>
                //{
                //    using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
                //    {
                //        conn.Open();
                //        var cmd = new NpgsqlCommand(@"INSERT INTO temploader (Model) VALUES (@p)", conn);
                //        cmd.Parameters.Add(new NpgsqlParameter("p", NpgsqlDbType.Jsonb) { Value = model });
                //        cmd.ExecuteNonQuery();
                //    }

                //});



                using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
                {
                    var someValue = new RootObject();
                    conn.Open();
                    var cmd = new NpgsqlCommand(@"SELECT model FROM temploader", conn);
                    var reader = cmd.ExecuteReader();
                    List<RootObject> fixtureList = new List<RootObject>();
                    while (reader.Read())
                    {
                        someValue = reader.GetFieldValue<RootObject>(0);
                        fixtureList.Add(someValue);
                    }
                    reader.Close();
                    cmd.Dispose();
                    MontaEstrutura(fixtureList, conn);

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

        private static void MontaEstrutura(List<RootObject> fixtureList, NpgsqlConnection conn)
        {
            //Criar lógica pra ver se já existe
            foreach (var item in fixtureList)
            {
                var fixtures = item.data.fixtures;
                var competitions = fixtures.GroupBy(x => x.competition.name);
                competitions.ToList().ForEach(c =>
                {
                    var exists = conn.QueryFirstOrDefault<bool>("SELECT 1 from Competition WHERE name = @key", new { key = c.Key });
                    if (!exists)
                    {
                        var cmd = new NpgsqlCommand($"INSERT INTO Competition (Name) VALUES ('{c.Key}')", conn);
                        cmd.ExecuteNonQuery();
                    }
                });

                var clubesHome = fixtures.GroupBy(x => x.home_name);
                var clubesAway = fixtures.GroupBy(x => x.away_name);

                var clubesString = clubesHome.Union(clubesAway).Select(x => x.Key).Distinct();
                List<string> insertClube = new List<string>();
                clubesString.ToList().ForEach(clube =>
                {
                    var exists = conn.QueryFirstOrDefault<bool>("SELECT 1 from Club WHERE name = @key", new { key = clube });
                    if (!exists)
                    {
                        var cmd = new NpgsqlCommand($"INSERT INTO Club (Name) VALUES ('{clube}')", conn);
                        cmd.ExecuteNonQuery();
                    }

                });


                var clubes = GetInsertedClubs(conn);
                var competitionsInserted = GetInsertedCompetitions(conn);

                List<Partida> listaDePartidas = new List<Partida>();
                fixtures.AsParallel().ForAll(p =>
                {
                    listaDePartidas.Add(new Partida
                    {
                        CompetitionId = GetCompetitionByName(p.competition.name, competitionsInserted).Id,
                        Date = Convert.ToDateTime(p.date).Date.Add(Convert.ToDateTime(p.time).TimeOfDay),
                        Location = p.location,
                        Team_Away_Id = GetTeamByName(p.away_name, clubes).Id,
                        Team_Home_Id = GetTeamByName(p.home_name, clubes).Id,
                        FixtureRound = Convert.ToInt32(p.round)
                    });
                });

                listaDePartidas.ForEach(p =>
                {
                    var parametros = new { homeId = p.Team_Home_Id, awayId = p.Team_Away_Id, competition = p.CompetitionId, round = p.FixtureRound };
                    string sql = @"SELECT 1 from Fixture 
                                    WHERE TeamHomeId = @homeId and TeamAwayId = @awayId and 
                                    CompetitionId = @competition and FixtureRound = @round";
                    var exists = conn.QueryFirstOrDefault<bool>(sql, parametros);
                    if (!exists)
                    {
                        var cmd = new NpgsqlCommand($@"SET datestyle = dmy;  INSERT INTO Fixture (TeamHomeId, TeamAwayId, Location, CompetitionId, FixtureRound, Date) 
                                            VALUES ({p.Team_Home_Id}, {p.Team_Away_Id}, '{p.Location}', {p.CompetitionId}, {p.FixtureRound}, '{p.Date}')", conn);
                        cmd.ExecuteNonQuery();
                    }
                });
            }

        }

        private static List<Model.Competition> GetInsertedCompetitions(NpgsqlConnection dbConnection)
        {

            return dbConnection.Query<Model.Competition>("SELECT * FROM Competition").AsList();

        }

        private static Model.Competition GetCompetitionByName(string name, List<Model.Competition> competitionsInserted)
        {
            return competitionsInserted?.FirstOrDefault(x => x.Name == name);
        }

        private static List<Clube> GetInsertedClubs(NpgsqlConnection dbConnection)
        {

            return dbConnection.Query<Clube>("SELECT * FROM Club").AsList();
        }

        private static Clube GetTeamByName(string name, List<Clube> clubes)
        {
            return clubes?.Where(x => x.Name == name).FirstOrDefault();
        }
    }
}
