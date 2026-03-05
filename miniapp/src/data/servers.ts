export interface Server {
  id: string;
  name: string;
  map: string;
  players: number;
  maxPlayers: number;
  ip: string;
  status: "online" | "offline";
}

export interface Player {
  id: string;
  nickname: string;
  kills: number;
  deaths: number;
  time: number; // в минутах
}

export const servers: Server[] = [
  {
    id: "1",
    name: "Strike.Uz | Public Style #1",
    map: "de_dust2",
    players: 19,
    maxPlayers: 32,
    ip: "83.69.139.205:27015",
    status: "online",
  },
  {
    id: "2",
    name: "Strike.Uz | Public Style #2",
    map: "de_inferno",
    players: 28,
    maxPlayers: 32,
    ip: "83.69.139.205:27016",
    status: "online",
  },
  {
    id: "3",
    name: "Strike.Uz | Dust2 Only",
    map: "de_dust2",
    players: 32,
    maxPlayers: 32,
    ip: "83.69.139.205:27017",
    status: "online",
  },
  {
    id: "4",
    name: "Strike.Uz | Aim Map",
    map: "aim_map",
    players: 12,
    maxPlayers: 20,
    ip: "83.69.139.205:27018",
    status: "online",
  },
  {
    id: "5",
    name: "Strike.Uz | Deathmatch",
    map: "de_nuke",
    players: 24,
    maxPlayers: 32,
    ip: "83.69.139.205:27019",
    status: "online",
  },
  {
    id: "6",
    name: "Strike.Uz | Zombie Mod",
    map: "zm_dust",
    players: 30,
    maxPlayers: 32,
    ip: "83.69.139.205:27020",
    status: "online",
  },
  {
    id: "7",
    name: "Strike.Uz | GunGame",
    map: "gg_simpsons",
    players: 15,
    maxPlayers: 24,
    ip: "83.69.139.205:27021",
    status: "online",
  },
  {
    id: "8",
    name: "Strike.Uz | Knife Arena",
    map: "ka_arena",
    players: 8,
    maxPlayers: 16,
    ip: "83.69.139.205:27022",
    status: "online",
  },
  {
    id: "9",
    name: "Strike.Uz | AWP Only",
    map: "awp_india",
    players: 20,
    maxPlayers: 24,
    ip: "83.69.139.205:27023",
    status: "online",
  },
  {
    id: "10",
    name: "Strike.Uz | Classic",
    map: "de_aztec",
    players: 16,
    maxPlayers: 32,
    ip: "83.69.139.205:27024",
    status: "online",
  },
  {
    id: "11",
    name: "Strike.Uz | VIP Only",
    map: "de_mirage",
    players: 22,
    maxPlayers: 32,
    ip: "83.69.139.205:27025",
    status: "online",
  },
  {
    id: "12",
    name: "Strike.Uz | Training",
    map: "de_train",
    players: 10,
    maxPlayers: 20,
    ip: "83.69.139.205:27026",
    status: "online",
  },
  {
    id: "13",
    name: "Strike.Uz | Fun Maps",
    map: "he_tennis",
    players: 14,
    maxPlayers: 24,
    ip: "83.69.139.205:27027",
    status: "online",
  },
  {
    id: "14",
    name: "Strike.Uz | Surf Server",
    map: "surf_ski",
    players: 18,
    maxPlayers: 24,
    ip: "83.69.139.205:27028",
    status: "online",
  },
  {
    id: "15",
    name: "Strike.Uz | Public Mix",
    map: "de_dust2_2x2",
    players: 4,
    maxPlayers: 10,
    ip: "83.69.139.205:27029",
    status: "online",
  },
];

export const getServerById = (id: string): Server | undefined => {
  return servers.find((server) => server.id === id);
};

export const generatePlayers = (count: number): Player[] => {
  const nicknames = [
    "Killer", "ProGamer", "Sniper", "Shadow", "Phantom",
    "Hunter", "Legend", "Master", "Warrior", "Champion",
    "Destroyer", "Predator", "Ninja", "Terminator", "Ghost",
    "Beast", "Rampage", "Viper", "Dragon", "Thunder",
    "Storm", "Blaze", "Frost", "Phoenix", "Reaper",
    "Savage", "Titan", "Wraith", "Rogue", "Slayer",
    "Falcon", "Havoc"
  ];

  return Array.from({ length: count }, (_, i) => ({
    id: `player-${i}`,
    nickname: nicknames[i % nicknames.length] + (i > nicknames.length - 1 ? i : ""),
    kills: Math.floor(Math.random() * 80) + 10,
    deaths: Math.floor(Math.random() * 50) + 5,
    time: Math.floor(Math.random() * 120) + 10,
  })).sort((a, b) => b.kills - a.kills);
};
