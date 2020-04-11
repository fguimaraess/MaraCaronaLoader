﻿using System;
using System.Threading.Tasks;
using HtmlAgilityPack;
using System.Net.Http;
using System.Linq;
using CrawlerGE.Model;
using Npgsql;
using Newtonsoft.Json;
using System.Collections;
using System.Collections.Generic;
using RestSharp;
using System.Net.Http.Headers;
using IdentityModel.Client;

namespace CrawlerGE
{
    class Program
    {
        private static readonly string _connectionString = @"Server=ec2-35-174-88-65.compute-1.amazonaws.com;Port=5432;" +
                    "User Id=odwvjyxqhzgabw;Password=ef7d9e8b0476a76423666b960558df83f905c75c2ca3e2321b7b52d96f9a63b9;Database=d85ko4e25vi0j6;SSL Mode=Require;Trust Server Certificate=true";
        static readonly HttpClient client = new HttpClient();

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

                    cmd.ExecuteNonQuery();

                    MontaEstrutura(partida, cmd);
                }
            }
            catch (Exception e)
            {
                throw e;
            }

        }

        private static void MontaEstrutura(PartidaDto partida, NpgsqlCommand conn)
        {
            var partidas = JsonConvert.DeserializeObject<IEnumerable<PartidaDto>>(partida.seriea);

            var clubesHome = partidas.GroupBy(x => x.Home_Name);
            var clubesAway = partidas.GroupBy(x => x.Away_Name);

            var clubes = new List<Clube>();
            var clubesString = clubesHome.Union(clubesAway).Select(x => x.Key);
            List<string> insertClube = new List<string>();
            clubesString.ToList().ForEach(clube =>
            {
                var cmd = new NpgsqlCommand($"INSERT INTO CLUBE (Name) VALUES ({clube})");
                cmd.ExecuteNonQuery();
            });


            List<Partida> listaDePartidas = new List<Partida>();
            partidas.AsParallel().ForAll(p =>
            {
                listaDePartidas.Add(new Partida
                {
                    Competition = p.Competition.Name,
                    Date = p.Date,
                    Location = p.Location,
                    Team_Away_Id = GetTeamByName(p.Away_Name, clubes).Id,
                    Team_Home_Id = GetTeamByName(p.Home_Name, clubes).Id
                });
            });
        }

        private static Clube GetTeamByName(string name, List<Clube> clubes)
        {
            return clubes?.Where(x => x.Name == name).FirstOrDefault();
        }
    }
}
