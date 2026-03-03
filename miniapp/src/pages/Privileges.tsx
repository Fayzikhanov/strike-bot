import { useState } from "react";
import { Link } from "react-router-dom";
import { privileges } from "../data/privileges";
import { Check } from "lucide-react";
import { PageTransition } from "../components/PageTransition";
import { isPurchasablePrivilegeId } from "../lib/purchaseRules";
import { useLanguage } from "../i18n/LanguageContext";

const uzFeatureMap: Record<string, string> = {
  "1 скин оружия (автоматически)": "1 ta qurol skin (avtomatik)",
  "1 скин персонажа (без выбора, автоматически)": "1 ta kiyim skin (tanlovsiz, avtomatik)",
  "1 скин ножа (без выбора, автоматически)": "1 ta pichoq skin (tanlovsiz, avtomatik)",
  "Просмотр нанесённого урона": "Uronni ko'rish imkoniyati",
  "+10 HP за убийство": "Kill uchun +10 HP",
  "Пакет гранат (без smoke и molotov)": "Granata paketi (smoke va molotovsiz)",
  "Шприц: +15 HP": "Shprits: +15 HP",
  "Стартовый HP: 110": "Boshlang'ich HP: 110",
  "Максимальный HP: 110": "Maks HP: 110",

  "1 скин оружия (лучше, чем у VIP)": "1 ta qurol skin (VIP’dan biroz yaxshiroq)",
  "1 скин персонажа (без выбора, автоматически, лучше VIP)": "1 ta kiyim skin (tanlovsiz, avtomatik, VIP’dan biroz yaxshiroq)",
  "1 скин ножа (без выбора, автоматически, лучше VIP)": "1 ta pichoq skin (tanlovsiz, avtomatik, VIP’dan biroz yaxshiroq)",
  "+20 HP за убийство": "Kill uchun +20 HP",
  "Пакет гранат + molotov": "Granata paketi + molotov",
  "Шприц: +50 HP": "Shprits: +50 HP",
  "HP: 120": "HP: 120",
  "Максимальный HP: 120": "Maks HP: 120",

  "1 скин оружия (лучше, чем у PRIME)": "1 ta qurol skin (PRIME’dan yaxshiroq)",
  "1 скин персонажа (без выбора, автоматически, лучше PRIME)": "1 ta kiyim skin (tanlovsiz, avtomatik, PRIME’dan yaxshiroq)",
  "3 скина ножа (с выбором, лучше PRIME)": "3 ta pichoq skin (tanlovli, PRIME’dan yaxshiroq)",
  "+30 HP за убийство": "Kill uchun +30 HP",
  "Пакет гранат + molotov + smoke": "Granata paketi + molotov + Smoke",
  "Шприц: +100 HP": "Shprits: +100 HP",
  "HP: 130": "HP: 130",
  "Максимальный HP: 130": "Maks HP: 130",
  AUTOBHOP: "AUTOBHOP",

  "Права Kick/Ban": "Kick/Ban huquqlari",
  "Mute/Gag игроков": "O'yinchilarni mute/gag qilish",
  "Управление голосованием за карту": "Xarita ovoz berishini boshqarish",
  "Инструменты модератора сервера": "Server moderatori vositalari",
  "Значок модератора": "Moderator belgisi",

  "Все возможности MODER": "MODER'ning barcha imkoniyatlari",
  "Полные права администратора": "To'liq administrator huquqlari",
  "Расширенная система банов": "Kengaytirilgan ban tizimi",
  "Резервный слот": "Rezerv slot",

  "Все возможности ADMIN": "ADMIN'ning barcha imkoniyatlari",
  "Глобальные права на сервере": "Serverda global huquqlar",
  "Расширенные права старшего админа": "Katta adminning keng huquqlari",
  "Управление командой админов": "Adminlar jamoasini boshqarish",
  "Модерация сообщества": "Hamjamiyat moderatsiyasi",
  "Специальный GL-значок": "Maxsus GL belgisi",

  "Управление CW/MIX серверами": "CW/MIX serverlarini boshqarish",
  "Инструменты настройки матчей": "Match sozlash vositalari",
  "Управление командами": "Jamoalarni boshqarish",
  "CWAR меню": "CWAR menyu",
  "Проведение турниров": "Turnir o'tkazish",
};

const uzPriceMap: Record<string, string> = {
  vip: "15 000 UZS / oydan",
  prime: "49 000 UZS / oy",
  legend: "79 000 UZS / oy",
  moder: "60 000 UZS / oydan",
  admin: "100 000 UZS / oy",
  "gl-admin": "Kelishuv asosida",
  "admin-cw": "Ariza bo'yicha",
};

export function Privileges() {
  const { language } = useLanguage();
  const [selectedPrivilege, setSelectedPrivilege] = useState(privileges[0]);
  const purchaseLink = isPurchasablePrivilegeId(selectedPrivilege.id)
    ? `/purchase?privilege=${encodeURIComponent(selectedPrivilege.id)}`
    : "/purchase";
  const isUz = language === "uz";
  const translatedFeatures = isUz
    ? selectedPrivilege.features.map((feature) => uzFeatureMap[feature] ?? feature)
    : selectedPrivilege.features;
  const translatedPrice = isUz
    ? (uzPriceMap[selectedPrivilege.id] ?? selectedPrivilege.price)
    : selectedPrivilege.price;

  return (
    <PageTransition>
      <div className="px-3 py-4 pb-24">
        <div className="space-y-4">
          <div className="text-center">
            <h1 className="text-[#FCFCFC] mb-1 text-xl">
              {isUz ? "Server imtiyozlari" : "Привилегии серверов"}
            </h1>
            <p className="text-[#888888] text-sm">
              {isUz
                ? "O'yiningiz uchun mos imtiyozni tanlang"
                : "Выберите подходящую привилегию для вашей игры"}
            </p>
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-2">
            <div className="grid grid-cols-2 gap-2">
              {privileges.map((privilege) => (
                <button
                  key={privilege.id}
                  onClick={() => setSelectedPrivilege(privilege)}
                  className={`px-3 py-3 rounded-lg font-bold transition-all ${
                    selectedPrivilege.id === privilege.id
                      ? "bg-[#F08800] text-[#121212] shadow-lg shadow-[#F08800]/30 scale-105"
                      : "bg-[#121212] text-[#888888] hover:text-[#FCFCFC] hover:bg-[#2a2a2a] active:scale-95"
                  } ${privilege.id === "admin-cw" ? "col-span-2" : ""}`}
                >
                  <div className="mb-1 h-8 flex items-center justify-center">
                    {privilege.iconImage ? (
                      <img
                        src={privilege.iconImage}
                        alt={`${privilege.name} icon`}
                        className="w-8 h-8 object-contain"
                      />
                    ) : (
                      <span className="text-2xl leading-none">{privilege.icon}</span>
                    )}
                  </div>
                  <div className="text-xs uppercase tracking-wide">{privilege.name}</div>
                </button>
              ))}
            </div>
          </div>

          <div
            className="bg-gradient-to-br from-[#1a1a1a] to-[#121212] border-2 rounded-lg overflow-hidden"
            style={{ borderColor: selectedPrivilege.color }}
          >
            <div className="p-5">
              <div className="text-center mb-5">
                <div className="mb-3 flex items-center justify-center">
                  {selectedPrivilege.iconImage ? (
                    <img
                      src={selectedPrivilege.iconImage}
                      alt={`${selectedPrivilege.name} icon`}
                      className="w-28 h-28 object-contain"
                    />
                  ) : (
                    <span className="text-5xl">{selectedPrivilege.icon}</span>
                  )}
                </div>
                <h2
                  className="mb-2 text-xl"
                  style={{ color: selectedPrivilege.color }}
                >
                  {selectedPrivilege.name}
                </h2>
                <div className="inline-block bg-[#2a2a2a] px-4 py-2 rounded-lg">
                  <span className="text-[#F08800] font-bold text-base">
                    {translatedPrice}
                  </span>
                </div>
              </div>

              <div className="space-y-2.5">
                {translatedFeatures.map((feature, index) => (
                  <div
                    key={index}
                    className="flex items-start gap-2.5 bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-3"
                  >
                    <div
                      className="flex-shrink-0 w-5 h-5 rounded-full flex items-center justify-center mt-0.5"
                      style={{ backgroundColor: `${selectedPrivilege.color}20` }}
                    >
                      <Check
                        className="w-3 h-3"
                        strokeWidth={3}
                        style={{ color: selectedPrivilege.color }}
                      />
                    </div>
                    <span className="text-[#FCFCFC] flex-1 text-sm">{feature}</span>
                  </div>
                ))}
              </div>

              <div className="mt-6">
                <Link
                  to={purchaseLink}
                  className="block w-full bg-[#F08800] hover:bg-[#d97700] text-[#121212] font-black py-4 rounded-lg text-center transition-all active:scale-95 shadow-lg shadow-[#F08800]/30 uppercase tracking-wide"
                >
                  {isUz ? `${selectedPrivilege.name} sotib olish` : `Купить ${selectedPrivilege.name}`}
                </Link>
              </div>
            </div>
          </div>

          <div className="bg-[#F08800]/10 border border-[#F08800]/30 rounded-lg p-3.5">
            <p className="text-[#888888] text-xs text-center leading-relaxed">
              <span className="text-[#F08800] font-bold">
                {isUz ? "Muhim:" : "Важно:"}
              </span>{" "}
              {isUz
                ? "Barcha imtiyozlar akkauntga biriktiriladi va faqat tanlangan Strike.Uz serverida ishlaydi."
                : "Все привилегии привязываются к аккаунту и работает только на выбранном сервере Strike.Uz."}
            </p>
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
