import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { ArrowLeft, Copy, Users, MapPin, Skull, Clock } from "lucide-react";
import { PageTransition } from "../components/PageTransition";
import {
  fetchServerPlayers,
  type LivePlayer,
  type LiveServer,
} from "../api/strikeApi";

export function ServerDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [copied, setCopied] = useState(false);
  const [server, setServer] = useState<LiveServer | null>(null);
  const [players, setPlayers] = useState<LivePlayer[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadServerData = useCallback(
    async (showLoader: boolean) => {
      if (!id) {
        return;
      }

      if (showLoader) {
        setIsLoading(true);
      }

      try {
        const payload = await fetchServerPlayers(id);
        setServer(payload.server);
        setPlayers(payload.players);
        setError(null);
      } catch {
        setError("Failed to load server details");
      } finally {
        if (showLoader) {
          setIsLoading(false);
        }
      }
    },
    [id],
  );

  useEffect(() => {
    if (!id) {
      setIsLoading(false);
      return;
    }

    void loadServerData(true);

    const timer = setInterval(() => {
      void loadServerData(false);
    }, 10000);

    return () => clearInterval(timer);
  }, [id, loadServerData]);

  const sortedPlayers = useMemo(
    () => [...players].sort((a, b) => b.kills - a.kills),
    [players],
  );

  const handleCopyIP = () => {
    if (!server) {
      return;
    }

    navigator.clipboard.writeText(server.ip);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (isLoading && !server) {
    return (
      <PageTransition>
        <div className="px-3 py-4 pb-24">
          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 animate-pulse">
            <div className="h-6 bg-[#2a2a2a] rounded w-3/4 mb-4" />
            <div className="h-4 bg-[#2a2a2a] rounded w-1/2 mb-2" />
            <div className="h-4 bg-[#2a2a2a] rounded w-2/3" />
          </div>
        </div>
      </PageTransition>
    );
  }

  if (!server) {
    return (
      <PageTransition>
        <div className="px-3 py-6">
          <div className="text-center text-[#888888]">{error ?? "Server not found"}</div>
        </div>
      </PageTransition>
    );
  }

  return (
    <PageTransition>
      <div className="px-3 py-4 pb-24">
        <button
          onClick={() => navigate("/")}
          className="flex items-center gap-2 text-[#888888] hover:text-[#F08800] transition-colors mb-4 active:scale-95"
        >
          <ArrowLeft className="w-5 h-5" strokeWidth={2.5} />
          <span className="font-bold">Back</span>
        </button>

        {error && (
          <div className="bg-[#1a1a1a] border border-[#ef4444]/40 rounded-lg p-3 mb-4 text-sm text-[#FCFCFC]">
            {error}
          </div>
        )}

        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-4">
          <div className="flex items-start gap-3 mb-4">
            <div
              className={`w-4 h-4 rounded-full flex-shrink-0 mt-1 ${
                server.status === "online"
                  ? "bg-green-500 shadow-lg shadow-green-500/50 animate-pulse"
                  : "bg-red-500"
              }`}
            />
            <div className="flex-1">
              <h2 className="text-[#FCFCFC] mb-4 text-lg">{server.name}</h2>

              <div className="space-y-3">
                <div className="flex items-center gap-2 text-[#888888]">
                  <MapPin className="w-5 h-5 flex-shrink-0" strokeWidth={2} />
                  <span className="text-[#FCFCFC] font-semibold">Map:</span>
                  <span className="text-[#F08800] font-bold">{server.map}</span>
                </div>

                <div className="flex items-center gap-2 text-[#888888]">
                  <Users className="w-5 h-5 flex-shrink-0" strokeWidth={2} />
                  <span className="text-[#FCFCFC] font-semibold">Players:</span>
                  <span className="text-[#F08800] font-bold">
                    {server.players}/{server.maxPlayers}
                  </span>
                </div>

                <div className="flex items-center gap-2 bg-[#121212] rounded-lg p-3">
                  <span className="text-[#888888] font-semibold text-xs">IP:</span>
                  <span className="text-[#FCFCFC] font-mono text-xs flex-1 break-all">
                    {server.ip}
                  </span>
                  <button
                    onClick={handleCopyIP}
                    className="p-2 text-[#888888] hover:text-[#F08800] hover:bg-[#2a2a2a] rounded transition-colors flex-shrink-0 active:scale-95"
                  >
                    {copied ? (
                      <span className="text-green-500 text-xs font-bold px-1">✓</span>
                    ) : (
                      <Copy className="w-4 h-4" strokeWidth={2} />
                    )}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
          <h3 className="text-[#FCFCFC] mb-4 flex items-center gap-2 font-bold">
            <Users className="w-5 h-5 text-[#F08800]" strokeWidth={2.5} />
            Players on Server
          </h3>

          {sortedPlayers.length === 0 ? (
            <div className="text-center text-[#888888] py-4">No active players</div>
          ) : (
            <div className="space-y-2">
              {sortedPlayers.map((player, index) => (
                <div
                  key={player.id}
                  className={`flex items-center gap-2 p-3 rounded-lg transition-colors ${
                    index === 0
                      ? "bg-[#F08800]/10 border border-[#F08800]/30"
                      : "bg-[#121212] hover:bg-[#2a2a2a]"
                  }`}
                >
                  <div className="w-6 text-center flex-shrink-0">
                    <span
                      className={`text-xs font-bold ${
                        index === 0 ? "text-[#F08800]" : "text-[#888888]"
                      }`}
                    >
                      #{index + 1}
                    </span>
                  </div>

                  <div className="flex-1 min-w-0">
                    <span
                      className={`font-bold truncate block text-sm ${
                        index === 0 ? "text-[#F08800]" : "text-[#FCFCFC]"
                      }`}
                    >
                      {player.nickname}
                    </span>
                  </div>

                  <div className="flex items-center gap-2 text-xs">
                    <div className="flex items-center gap-1">
                      <Skull className="w-3.5 h-3.5 text-[#888888]" strokeWidth={2} />
                      <span className="text-[#FCFCFC] font-bold min-w-[1.5rem] text-right">
                        {player.kills}
                      </span>
                    </div>

                    <div className="flex items-center gap-1">
                      <div className="w-3.5 h-3.5 flex items-center justify-center">
                        <div className="w-1.5 h-1.5 bg-red-500 rounded-full" />
                      </div>
                      <span className="text-[#888888] font-bold min-w-[1.5rem] text-right">
                        {player.deaths ?? "-"}
                      </span>
                    </div>

                    <div className="flex items-center gap-1">
                      <Clock className="w-3.5 h-3.5 text-[#888888]" strokeWidth={2} />
                      <span className="text-[#888888] font-semibold min-w-[2rem] text-right">
                        {player.time}m
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}
