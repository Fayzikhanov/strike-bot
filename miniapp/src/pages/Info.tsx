import { Server, Shield, Users, Trophy } from "lucide-react";
import { PageTransition } from "../components/PageTransition";
import { useLanguage } from "../i18n/LanguageContext";

export function Info() {
  const { language } = useLanguage();
  const isUz = language === "uz";

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

  return (
    <PageTransition>
      <div className="px-3 py-4 pb-24">
        <div className="space-y-4">
          {/* Hero section */}
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

          {/* About section */}
          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <h2 className="text-[#F08800] mb-3 text-lg">{isUz ? "Loyiha haqida" : "О проекте"}</h2>
            <div className="space-y-3 text-[#888888] text-sm">
              {aboutParagraphs.map((paragraph) => (
                <p key={paragraph}>{paragraph}</p>
              ))}
            </div>
          </div>

          {/* Features grid */}
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

          {/* Game modes */}
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

          {/* Stats */}
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

          {/* Contact */}
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
