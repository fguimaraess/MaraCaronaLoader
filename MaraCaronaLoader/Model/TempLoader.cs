using System;
using System.Collections.Generic;
using System.Text;

namespace MaraCaronaLoader.Model
{
    public class TempLoader
    {
        public int Id { get; set; }
        public string Model { get; set; }
    }

    public class League
    {
        public string id { get; set; }
        public string name { get; set; }
        public string country_id { get; set; }
    }

    public class Competition
    {
        public string id { get; set; }
        public string name { get; set; }
    }

    public class Fixture
    {
        public string id { get; set; }
        public string date { get; set; }
        public string time { get; set; }
        public string round { get; set; }
        public League league { get; set; }
        public string away_id { get; set; }
        public string home_id { get; set; }
        public string location { get; set; }
        public string away_name { get; set; }
        public string home_name { get; set; }
        public string league_id { get; set; }
        public Competition competition { get; set; }
        public string competition_id { get; set; }
    }

    public class Data
    {
        public List<Fixture> fixtures { get; set; }
        //public string next_page { get; set; }
        //public dynamic prev_page { get; set; }
    }

    public class RootObject
    {
        public Data data { get; set; }
        public bool success { get; set; }
    }
}
