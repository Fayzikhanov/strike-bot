import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { MapPin, Server, Shield, Trophy, Users } from "lucide-react";
import { PageTransition } from "../components/PageTransition";
import { fetchServers, type LiveServer } from "../api/strikeApi";
import { isPublicServer } from "../lib/purchaseRules";
import { useLanguage } from "../i18n/LanguageContext";

export function ServersList() {
  const { language } = useLanguage();
  const isUz = language === "uz";

  const [servers, setServers] = useState<LiveServer[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadServers = useCallback(async (showLoader: boolean) => {
    if (showLoader) {
      setIsLoading(true);
    }

    try {
      const liveServers = await fetchServers();
      setServers(liveServers);
      setError(null);
    } catch {
      setError("Failed to load live server data");
    } finally {
      if (showLoader) {
        setIsLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void loadServers(true);

    const timer = setInterval(() => {
      void loadServers(false);
    }, 15000);

    return () => clearInterval(timer);
  }, [loadServers]);

  const aboutParagraphs = isUz
    ? [
        "Strike.Uz O'zbekistondagi eng yirik Counter-Strike 1.6 hamjamiyatlaridan biri bo'lib, sifatli serverlar, past ping va professional administratsiya bilan ajralib turadi.",
        "Biz turli o'yin rejimlarini taklif qilamiz: Public, Klassika, DeathMatch, HNS va boshqalar. Serverlarimiz 24/7 barqaror va qulay o'yin uchun sozlangan.",
        "Har kuni Strike.Uz'ni tanlayotgan minglab o'yinchilar safiga qo'shiling va Counter-Strike 1.6 dan zavq oling.",
      ]
    : [
        "Strike.Uz это одно из крупнейших сообществ Counter-Strike 1.6 в Узбекистане с качественными серверами, низким пингом и профессиональной администрацией.",
        "Мы предлагаем разные режимы игры: Паблики, Классика, Дезматч, HNS и другие. Наши серверы настроены для стабильной работы и комфортной игры 24/7.",
        "Присоединяйтесь к тысячам игроков, которые каждый день выбирают Strike.Uz для игры в Counter-Strike 1.6.",
      ];

  const modes = ["Public", "MIX 5x5", "DeathMatch", "HNS", "Only Dust", isUz ? "Boshqa" : "Другие"];
  const publicServers = servers.filter(isPublicServer);
  const mixServers = servers.filter((server) => !isPublicServer(server));

  const renderCardSkeleton = (index: number) => (
    <div
      key={`skeleton-${index}`}
      className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-3.5 animate-pulse"
    >
      <div className="h-5 bg-[#2a2a2a] rounded w-3/4 mb-3" />
      <div className="h-4 bg-[#2a2a2a] rounded w-1/2" />
    </div>
  );

  const renderServerCard = (server: LiveServer) => (
    <Link key={server.id} to={`/server/${server.id}`} className="block">
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-3.5 transition-all hover:border-[#F08800]/50 hover:shadow-lg hover:shadow-[#F08800]/10 active:scale-[0.98] active:bg-[#2a2a2a]">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0 pt-1">
            <div
              className={`w-3 h-3 rounded-full ${
                server.status === "online"
                  ? "bg-green-500 shadow-lg shadow-green-500/50 animate-pulse"
                  : "bg-red-500"
              }`}
            />
          </div>

          <div className="flex-1 min-w-0">
            <h3 className="text-[#FCFCFC] font-bold mb-2 truncate text-base">
              {server.name}
            </h3>

            <div className="flex flex-wrap items-center gap-3 text-sm">
              <div className="flex items-center gap-1.5 text-[#888888]">
                <Users className="w-4 h-4 flex-shrink-0" strokeWidth={2} />
                <span className="text-[#FCFCFC] font-bold">
                  {server.players}/{server.maxPlayers}
                </span>
              </div>

              <div className="flex items-center gap-1.5 text-[#888888]">
                <MapPin className="w-4 h-4 flex-shrink-0" strokeWidth={2} />
                <span className="text-[#F08800] font-semibold">{server.map}</span>
              </div>
            </div>
          </div>

          <div className="flex-shrink-0 text-[#F08800]">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2.5}
                d="M9 5l7 7-7 7"
              />
            </svg>
          </div>
        </div>
      </div>
    </Link>
  );

  const renderServerSection = (
    title: string,
    sectionServers: LiveServer[],
  ) => (
    <section className="space-y-2.5" key={title}>
      <div className="px-1 pt-1">
        <h2 className="text-[#FCFCFC] text-xl font-black uppercase tracking-wide">{title}</h2>
      </div>
      <div className="space-y-2.5">{sectionServers.map(renderServerCard)}</div>
    </section>
  );

  return (
    <PageTransition>
      <div className="px-3 py-4 pb-24">
        <div className="space-y-4">
          <div className="bg-gradient-to-br from-[#1a1a1a] to-[#121212] border border-[#F08800]/30 rounded-lg p-6 text-center">
            <div className="inline-block mb-3">
              <div className="w-16 h-16 bg-[#F08800]/10 rounded-full flex items-center justify-center">
                <Server className="w-8 h-8 text-[#F08800]" strokeWidth={2} />
              </div>
            </div>
            <h1 className="text-[#FCFCFC] mb-2 text-2xl">Strike.Uz</h1>
            <p className="text-[#888888]">
              {isUz
                ? "Counter-Strike 1.6 uchun premium o'yin tarmog'i"
                : "Премиальная игровая сеть Counter-Strike 1.6"}
            </p>
          </div>

          {error && servers.length === 0 ? (
            <div className="bg-[#1a1a1a] border border-[#ef4444]/40 rounded-lg p-4 text-center">
              <p className="text-[#FCFCFC] mb-3">{error}</p>
              <button
                onClick={() => void loadServers(true)}
                className="bg-[#F08800] text-[#121212] font-bold px-4 py-2 rounded-lg"
              >
                {isUz ? "Qayta urinish" : "Повторить"}
              </button>
            </div>
          ) : (
            <div className="space-y-4">
              {isLoading && servers.length === 0
                ? [0, 1, 2, 3, 4].map(renderCardSkeleton)
                : (
                    <>
                      {publicServers.length > 0 &&
                        renderServerSection(
                          isUz ? "Public serverlar" : "Public Servers",
                          publicServers,
                        )}
                      {mixServers.length > 0 &&
                        renderServerSection(
                          isUz ? "MIX serverlar" : "MIX Servers",
                          mixServers,
                        )}
                    </>
                  )}
            </div>
          )}

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <h2 className="text-[#F08800] mb-3 text-lg">{isUz ? "Loyiha haqida" : "О проекте"}</h2>
            <div className="space-y-3 text-[#888888] text-sm">
              {aboutParagraphs.map((paragraph) => (
                <p key={paragraph}>{paragraph}</p>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-1 gap-3">
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3">
                <div className="flex-shrink-0 w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center">
                  <Shield className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <div className="flex-1">
                  <h3 className="text-[#FCFCFC] mb-1 font-bold text-sm">{isUz ? "ADMINLAR" : "АДМИНЫ"}</h3>
                  <p className="text-[#888888] text-xs">
                    {isUz
                      ? "Tezkor administratsiya sizni cheaterlardan himoya qiladi"
                      : "Своевременная администрация защитит вас от читеров"}
                  </p>
                </div>
              </div>
            </div>

            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3">
                <div className="flex-shrink-0 w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center">
                  <Users className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <div className="flex-1">
                  <h3 className="text-[#FCFCFC] mb-1 font-bold text-sm">
                    {isUz ? "Faol hamjamiyat" : "Активное сообщество"}
                  </h3>
                  <p className="text-[#888888] text-xs">
                    {isUz ? "Har kuni minglab faol o'yinchilar" : "Тысячи активных игроков каждый день"}
                  </p>
                </div>
              </div>
            </div>

            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3">
                <div className="flex-shrink-0 w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center">
                  <Trophy className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <div className="flex-1">
                  <h3 className="text-[#FCFCFC] mb-1 font-bold text-sm">
                    {isUz ? "TURNIRLAR" : "ТУРНИРЫ"}
                  </h3>
                  <p className="text-[#888888] text-xs">
                    {isUz
                      ? "Muntazam musobaqalar va sovrinli tanlovlar"
                      : "Регулярные соревнования и конкурсы с призами"}
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <h2 className="text-[#F08800] mb-3 text-lg">{isUz ? "O'yin rejimlari" : "Режимы игры"}</h2>
            <div className="grid grid-cols-2 gap-2">
              {modes.map((mode) => (
                <div
                  key={mode}
                  className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-2.5 text-center"
                >
                  <span className="text-[#FCFCFC] font-bold text-sm">{mode}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-gradient-to-r from-[#F08800]/10 to-[#F08800]/5 border border-[#F08800]/30 rounded-lg p-5">
            <div className="grid grid-cols-3 gap-4 text-center">
              <div>
                <div className="text-3xl font-bold text-[#F08800] mb-1">15+</div>
                <div className="text-[#888888] text-xs font-semibold">
                  {isUz ? "Serverlar" : "Серверов"}
                </div>
              </div>
              <div>
                <div className="text-3xl font-bold text-[#F08800] mb-1">24/7</div>
                <div className="text-[#888888] text-xs font-semibold">
                  {isUz ? "Aptime" : "Аптайм"}
                </div>
              </div>
              <div>
                <div className="text-3xl font-bold text-[#F08800] mb-1">3000+</div>
                <div className="text-[#888888] text-xs font-semibold">
                  {isUz ? "O'yinchilar" : "Игроков"}
                </div>
              </div>
            </div>
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <h2 className="text-[#F08800] mb-3 text-lg">{isUz ? "Kontaktlar" : "Контакты"}</h2>
            <div className="space-y-2.5 text-sm">
              <div className="flex items-center justify-between bg-[#121212] rounded-lg p-3">
                <span className="text-[#888888]">
                  {isUz ? "Telegram kanal:" : "Telegram канал:"}
                </span>
                <a
                  href="https://t.me/STRIKEUZCHANNEL"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#F08800] font-bold hover:underline"
                >
                  @STRIKEUZCHANNEL
                </a>
              </div>
              <div className="flex items-center justify-between bg-[#121212] rounded-lg p-3">
                <span className="text-[#888888]">
                  {isUz ? "Telegram guruh:" : "Telegram группа:"}
                </span>
                <a
                  href="https://t.me/STRIKEUZCOMMUNITY"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#F08800] font-bold hover:underline"
                >
                  @STRIKEUZCOMMUNITY
                </a>
              </div>
              <div className="flex items-center justify-between bg-[#121212] rounded-lg p-3">
                <span className="text-[#888888]">
                  {isUz ? "MIX 5x5 guruh:" : "MIX 5x5 группа:"}
                </span>
                <a
                  href="https://t.me/STRIKECW"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#F08800] font-bold hover:underline"
                >
                  @STRIKECW
                </a>
              </div>
              <div className="flex items-center justify-between bg-[#121212] rounded-lg p-3">
                <span className="text-[#888888]">
                  {isUz ? "Banlar va hisobotlar:" : "Баны и отчёты:"}
                </span>
                <a
                  href="https://t.me/STRIKEUZREPORTS"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#F08800] font-bold hover:underline"
                >
                  @STRIKEUZREPORTS
                </a>
              </div>
              <div className="flex items-center justify-between bg-[#121212] rounded-lg p-3">
                <span className="text-[#888888]">{isUz ? "Yordam:" : "Поддержка:"}</span>
                <a
                  href="https://t.me/MCCALLSTRIKE"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#F08800] font-bold hover:underline"
                >
                  @MCCALLSTRIKE
                </a>
              </div>
            </div>
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
