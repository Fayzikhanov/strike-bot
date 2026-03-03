import { useCallback, useEffect, useMemo, useState } from "react";
import { ChevronDown, Clock3, PlusCircle, WalletCards } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { PageTransition } from "../components/PageTransition";
import { useLanguage } from "../i18n/LanguageContext";
import {
  fetchUserBalance,
  fetchUserBalanceHistory,
  type BalanceHistoryItem,
  fetchUserPrivileges,
  type UserPrivilegeItem,
} from "../api/strikeApi";
import { useBalanceTopUp } from "../context/BalanceTopUpContext";

type TelegramUser = {
  id?: number;
  username?: string;
  first_name?: string;
  last_name?: string;
};

function formatMoney(value: number): string {
  return Math.max(0, Math.floor(value || 0)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}

function formatDateTime(unixSeconds: number, language: "ru" | "uz"): string {
  if (!Number.isFinite(unixSeconds) || unixSeconds <= 0) {
    return "-";
  }
  const locale = language === "uz" ? "uz-UZ" : "ru-RU";
  return new Date(unixSeconds * 1000).toLocaleString(locale, {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getMonthKey(unixSeconds: number): string {
  if (!Number.isFinite(unixSeconds) || unixSeconds <= 0) {
    return "unknown";
  }
  const date = new Date(unixSeconds * 1000);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function formatMonthKey(monthKey: string, language: "ru" | "uz"): string {
  if (!monthKey || monthKey === "all") {
    return language === "uz" ? "Barchasi" : "Все";
  }
  if (monthKey === "unknown") {
    return language === "uz" ? "Noma'lum" : "Неизвестно";
  }
  const [yearPart, monthPart] = monthKey.split("-");
  const year = Number(yearPart);
  const month = Number(monthPart);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return monthKey;
  }
  const date = new Date(year, month - 1, 1);
  return date.toLocaleString(language === "uz" ? "uz-UZ" : "ru-RU", {
    month: "long",
    year: "numeric",
  });
}

function getMetaString(meta: Record<string, unknown>, key: string): string {
  return String(meta[key] ?? "").trim();
}

function getMetaNumber(meta: Record<string, unknown>, key: string): number {
  const value = Number(meta[key] ?? 0);
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.floor(value);
}

function buildHistoryTitle(item: BalanceHistoryItem, language: "ru" | "uz"): string {
  const isUz = language === "uz";
  const meta = item.meta ?? {};
  const productType = getMetaString(meta, "product_type").toLowerCase();

  if (item.type === "topup") {
    return isUz ? "Balans to'ldirildi" : "Пополнение баланса";
  }
  if (item.type === "cashback") {
    return isUz ? "Keshbek qaytdi" : "Начислен кэшбек";
  }
  if (item.type === "purchase") {
    if (productType === "bonus") {
      return isUz ? "Bonuslar xaridi" : "Покупка бонусов";
    }
    return isUz ? "Imtiyoz xaridi" : "Покупка привилегии";
  }
  return isUz ? "Balans amaliyoti" : "Операция баланса";
}

function buildHistoryDetails(item: BalanceHistoryItem, language: "ru" | "uz"): string {
  const isUz = language === "uz";
  const meta = item.meta ?? {};
  const serverName = getMetaString(meta, "server_name");
  const privilege = getMetaString(meta, "privilege");
  const durationMonths = getMetaNumber(meta, "duration_months");
  const cashbackPercent = getMetaNumber(meta, "cashback_percent");
  const bonusAmount = getMetaNumber(meta, "bonus_amount");
  const productType = getMetaString(meta, "product_type").toLowerCase();

  if (item.type === "topup") {
    return isUz
      ? "Skrinshot tekshirilib, balansga qo'shildi."
      : "Проверено по скриншоту и зачислено на баланс.";
  }

  if (item.type === "cashback") {
    if (serverName || privilege) {
      return isUz
        ? `${privilege || "Imtiyoz"} (${serverName || "Server"}) bo'yicha ${cashbackPercent || 0}% keshbek.`
        : `Кэшбек ${cashbackPercent || 0}% за ${privilege || "привилегию"} (${serverName || "сервер"}).`;
    }
    return isUz ? "Xarid bo'yicha keshbek qo'shildi." : "Начислен кэшбек за покупку.";
  }

  if (item.type === "purchase") {
    if (productType === "bonus") {
      return isUz
        ? `Server: ${serverName || "-"} • Bonus: ${formatMoney(Math.abs(bonusAmount))}`
        : `Сервер: ${serverName || "-"} • Бонусов: ${formatMoney(Math.abs(bonusAmount))}`;
    }

    const parts: string[] = [];
    if (serverName) {
      parts.push(`${isUz ? "Server" : "Сервер"}: ${serverName}`);
    }
    if (privilege) {
      parts.push(`${isUz ? "Imtiyoz" : "Привилегия"}: ${privilege}`);
    }
    if (durationMonths > 0) {
      parts.push(`${isUz ? "Muddat" : "Срок"}: ${durationMonths} ${isUz ? "oy" : "мес."}`);
    }
    if (parts.length > 0) {
      return parts.join(" • ");
    }
    return isUz ? "Imtiyoz xaridi uchun yechib olindi." : "Списано за покупку привилегии.";
  }

  return isUz ? "Balans o'zgarishi." : "Изменение баланса.";
}

export function Profile() {
  const { language } = useLanguage();
  const isUz = language === "uz";
  const [balance, setBalance] = useState(0);
  const [isLoadingBalance, setIsLoadingBalance] = useState(false);
  const [balanceError, setBalanceError] = useState<string | null>(null);
  const [historyItems, setHistoryItems] = useState<BalanceHistoryItem[]>([]);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [privilegeItems, setPrivilegeItems] = useState<UserPrivilegeItem[]>([]);
  const [isLoadingPrivileges, setIsLoadingPrivileges] = useState(false);
  const [privilegesError, setPrivilegesError] = useState<string | null>(null);
  const [isPrivilegesExpanded, setIsPrivilegesExpanded] = useState(false);
  const [isLedgerExpanded, setIsLedgerExpanded] = useState(false);
  const [activeHistoryMonth, setActiveHistoryMonth] = useState("all");
  const [visibleHistoryCount, setVisibleHistoryCount] = useState(8);
  const { openTopUp } = useBalanceTopUp();
  const navigate = useNavigate();

  const user = useMemo(() => {
    return (
      (
        window as Window & {
          Telegram?: {
            WebApp?: {
              initDataUnsafe?: {
                user?: TelegramUser;
              };
            };
          };
        }
      ).Telegram?.WebApp?.initDataUnsafe?.user ?? null
    );
  }, []);

  const telegramUserId = useMemo(() => {
    const rawId = Number(user?.id ?? 0);
    if (!Number.isFinite(rawId) || rawId <= 0) {
      return 0;
    }
    return Math.floor(rawId);
  }, [user?.id]);

  const displayName = useMemo(() => {
    if (!user) {
      return isUz ? "Mehmon" : "Гость";
    }
    const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
    if (fullName) {
      return fullName;
    }
    if (user.username) {
      return `@${user.username.replace(/^@+/, "")}`;
    }
    return isUz ? "Foydalanuvchi" : "Пользователь";
  }, [isUz, user]);

  const loadBalance = useCallback(async (withLoader: boolean) => {
    if (telegramUserId <= 0) {
      setBalance(0);
      return;
    }
    if (withLoader) {
      setIsLoadingBalance(true);
    }
    try {
      const response = await fetchUserBalance(telegramUserId);
      setBalance(Math.max(0, Number(response.balance || 0)));
      setBalanceError(null);
    } catch {
      setBalanceError(
        isUz
          ? "Balansni yuklab bo'lmadi. Keyinroq qayta urinib ko'ring."
          : "Не удалось загрузить баланс. Попробуйте позже.",
      );
    } finally {
      if (withLoader) {
        setIsLoadingBalance(false);
      }
    }
  }, [isUz, telegramUserId]);

  const loadHistory = useCallback(async (withLoader: boolean) => {
    if (telegramUserId <= 0) {
      setHistoryItems([]);
      return;
    }
    if (withLoader) {
      setIsLoadingHistory(true);
    }
    try {
      const response = await fetchUserBalanceHistory(telegramUserId, 120);
      setHistoryItems(Array.isArray(response.items) ? response.items : []);
      setHistoryError(null);
    } catch {
      setHistoryError(
        isUz
          ? "Operatsiyalar tarixini yuklab bo'lmadi."
          : "Не удалось загрузить историю операций.",
      );
    } finally {
      if (withLoader) {
        setIsLoadingHistory(false);
      }
    }
  }, [isUz, telegramUserId]);

  const loadPrivileges = useCallback(async (withLoader: boolean) => {
    if (telegramUserId <= 0) {
      setPrivilegeItems([]);
      return;
    }
    if (withLoader) {
      setIsLoadingPrivileges(true);
    }
    try {
      const response = await fetchUserPrivileges(telegramUserId, 30);
      setPrivilegeItems(Array.isArray(response.items) ? response.items : []);
      setPrivilegesError(null);
    } catch {
      setPrivilegesError(
        isUz
          ? "Imtiyozlar ro'yxatini yuklab bo'lmadi."
          : "Не удалось загрузить список привилегий.",
      );
    } finally {
      if (withLoader) {
        setIsLoadingPrivileges(false);
      }
    }
  }, [isUz, telegramUserId]);

  useEffect(() => {
    void loadBalance(true);
    void loadHistory(true);
    void loadPrivileges(true);
  }, [loadBalance, loadHistory, loadPrivileges]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void loadPrivileges(false);
    }, 60000);
    return () => window.clearInterval(timer);
  }, [loadPrivileges]);

  useEffect(() => {
    const onTopUpSuccess = () => {
      void loadBalance(false);
      void loadHistory(false);
      void loadPrivileges(false);
    };
    window.addEventListener("strike:balance-topup-success", onTopUpSuccess);
    return () => {
      window.removeEventListener("strike:balance-topup-success", onTopUpSuccess);
    };
  }, [loadBalance, loadHistory, loadPrivileges]);

  const availableHistoryMonths = useMemo(() => {
    const monthSet = new Set<string>();
    for (const item of historyItems) {
      monthSet.add(getMonthKey(Number(item.createdAt || 0)));
    }
    return ["all", ...Array.from(monthSet).sort((a, b) => b.localeCompare(a))];
  }, [historyItems]);

  useEffect(() => {
    if (!availableHistoryMonths.includes(activeHistoryMonth)) {
      setActiveHistoryMonth("all");
    }
  }, [activeHistoryMonth, availableHistoryMonths]);

  const filteredHistoryItems = useMemo(() => {
    if (activeHistoryMonth === "all") {
      return historyItems;
    }
    return historyItems.filter(
      (item) => getMonthKey(Number(item.createdAt || 0)) === activeHistoryMonth,
    );
  }, [activeHistoryMonth, historyItems]);

  useEffect(() => {
    setVisibleHistoryCount(8);
  }, [activeHistoryMonth]);

  const visibleHistoryItems = useMemo(
    () => filteredHistoryItems.slice(0, visibleHistoryCount),
    [filteredHistoryItems, visibleHistoryCount],
  );

  const hasMoreHistoryItems = visibleHistoryCount < filteredHistoryItems.length;

  const handleRenewPrivilege = (item: UserPrivilegeItem) => {
    if (!item.canRenew || !item.serverId || !item.privilegeKey) {
      return;
    }
    const params = new URLSearchParams({
      server: item.serverId,
      privilege: item.privilegeKey,
      renew: "1",
    });
    if (item.identifierType === "steam" && item.steamId) {
      params.set("identifierType", "steam");
      params.set("steamId", item.steamId);
    } else {
      params.set("identifierType", "nickname");
      if (item.nickname) {
        params.set("nickname", item.nickname);
      }
    }
    navigate(`/purchase?${params.toString()}`);
  };

  return (
    <PageTransition>
      <div className="px-3 py-4 pb-24">
        <div className="space-y-4">
          <div className="bg-gradient-to-br from-[#1a1a1a] to-[#121212] border border-[#2a2a2a] rounded-lg p-4">
            <p className="text-[#888888] text-xs uppercase tracking-[0.15em]">
              {isUz ? "Profil" : "Профиль"}
            </p>
            <h1 className="text-[#FCFCFC] text-2xl font-black mt-1">{displayName}</h1>
          </div>

          <button
            type="button"
            onClick={openTopUp}
            className="w-full bg-gradient-to-r from-[#F08800] to-[#ff9f1a] rounded-xl p-5 text-left shadow-lg shadow-[#F08800]/20 transition-all active:scale-[0.99]"
          >
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-xl bg-[#121212]/15 border border-[#121212]/25 flex items-center justify-center">
                <WalletCards className="w-7 h-7 text-[#121212]" strokeWidth={2.3} />
              </div>
              <div className="min-w-0">
                <p className="text-[#121212]/80 text-xs uppercase tracking-[0.13em] font-bold">
                  {isUz ? "Balans" : "Баланс"}
                </p>
                <p className="text-[#121212] text-3xl font-black leading-none mt-1">
                  {isLoadingBalance ? "..." : `${formatMoney(balance)} UZS`}
                </p>
              </div>
            </div>
          </button>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <button
              type="button"
              onClick={openTopUp}
              className="w-full bg-[#121212] border border-[#F08800]/40 rounded-lg p-4 flex items-center justify-between transition-all hover:border-[#F08800]/70 active:scale-[0.99]"
            >
              <span className="text-[#FCFCFC] font-bold text-base">
                {isUz ? "Hisobni to'ldirish" : "Пополнить счёт"}
              </span>
              <PlusCircle className="w-5 h-5 text-[#F08800]" strokeWidth={2.2} />
            </button>

            {balanceError ? (
              <p className="text-[#fca5a5] text-xs leading-relaxed mt-3">{balanceError}</p>
            ) : (
              <p className="text-[#888888] text-xs leading-relaxed mt-3">
                {isUz
                  ? "Popolnenie skrinshot orqali tekshiriladi va balansga qo'shiladi."
                  : "Пополнение проверяется по скриншоту и зачисляется на баланс."}
              </p>
            )}
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-[#FCFCFC] text-base font-black">
                  {isUz ? "Faol imtiyozlar" : "Активные привилегии"}
                </h2>
                <p className="text-[#7f7f7f] text-[11px] mt-1">
                  {isUz
                    ? "Server, nick va qolgan kunlar."
                    : "Сервер, ник и оставшиеся дни."}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setIsPrivilegesExpanded((prev) => !prev)}
                className="inline-flex items-center gap-1.5 rounded-lg border border-[#2f2f2f] bg-[#121212] px-2.5 py-2 text-[#c9c9c9] text-xs font-bold uppercase tracking-wide"
              >
                {isPrivilegesExpanded
                  ? (isUz ? "Yopish" : "Скрыть")
                  : (isUz ? "Ochish" : "Открыть")}
                <ChevronDown
                  className={`w-3.5 h-3.5 transition-transform ${isPrivilegesExpanded ? "rotate-180" : ""}`}
                  strokeWidth={2.1}
                />
              </button>
            </div>

            {!isPrivilegesExpanded ? (
              <p className="text-[#888888] text-xs mt-3">
                {isUz ? "Imtiyozlar bloki yopilgan." : "Блок привилегий свернут."}
              </p>
            ) : isLoadingPrivileges ? (
              <p className="text-[#888888] text-xs mt-3">{isUz ? "Yuklanmoqda..." : "Загрузка..."}</p>
            ) : privilegesError ? (
              <p className="text-[#fca5a5] text-xs leading-relaxed mt-3">{privilegesError}</p>
            ) : privilegeItems.length === 0 ? (
              <p className="text-[#888888] text-xs mt-3">
                {isUz ? "Hozircha faol imtiyozlar yo'q." : "Активных привилегий пока нет."}
              </p>
            ) : (
              <div className="space-y-2 mt-3">
                {privilegeItems.map((item) => {
                  const safeTotalDays = Math.max(1, Number(item.totalDays || 0));
                  const safeRemainingDays = Math.max(0, Number(item.remainingDays || 0));
                  const progressPercent = Math.max(
                    0,
                    Math.min(100, Math.round((safeRemainingDays / safeTotalDays) * 100)),
                  );
                  const identifierValue = item.identifierType === "steam"
                    ? item.steamId
                    : item.nickname;
                  return (
                    <div key={item.id} className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-[#FCFCFC] text-sm font-black">
                          {item.privilegeLabel}
                        </p>
                        <p className="text-[#F8B24E] text-xs font-black">
                          {safeRemainingDays}/{safeTotalDays}
                        </p>
                      </div>
                      <p className="text-[#a3a3a3] text-xs mt-1">{item.serverName}</p>
                      <p className="text-[#d4d4d4] text-xs mt-1">
                        {item.identifierType === "steam" ? "STEAM_ID" : "Nick"}:{" "}
                        <span className="text-[#FCFCFC]">{identifierValue || "-"}</span>
                      </p>
                      <div className="mt-2 h-1.5 rounded-full bg-[#242424] overflow-hidden">
                        <div
                          className="h-full bg-gradient-to-r from-[#22c55e] to-[#86efac]"
                          style={{ width: `${progressPercent}%` }}
                        />
                      </div>
                      <button
                        type="button"
                        disabled={!item.canRenew}
                        onClick={() => handleRenewPrivilege(item)}
                        className={`mt-3 w-full rounded-lg border px-3 py-2 text-xs font-black uppercase tracking-wide transition-colors ${
                          item.canRenew
                            ? "bg-[#F08800]/12 border-[#F08800]/60 text-[#F8B24E]"
                            : "bg-[#1a1a1a] border-[#2a2a2a] text-[#6a6a6a] cursor-not-allowed"
                        }`}
                      >
                        {isUz ? "Imtiyozni uzaytirish" : "Продлить привилегию"}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-[#FCFCFC] text-base font-black">
                  {isUz ? "Hamyon jang jurnali" : "Боевой лог кошелька"}
                </h2>
                <p className="text-[#7f7f7f] text-[11px] mt-1">
                  {isUz
                    ? "To'ldirish, xarid va keshbek harakati."
                    : "Пополнения, покупки и кэшбек в одном логе."}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setIsLedgerExpanded((prev) => !prev)}
                className="inline-flex items-center gap-1.5 rounded-lg border border-[#2f2f2f] bg-[#121212] px-2.5 py-2 text-[#c9c9c9] text-xs font-bold uppercase tracking-wide"
              >
                <Clock3 className="w-3.5 h-3.5 text-[#888888]" strokeWidth={2.1} />
                {isLedgerExpanded
                  ? (isUz ? "Yopish" : "Скрыть")
                  : (isUz ? "Ochish" : "Открыть")}
                <ChevronDown
                  className={`w-3.5 h-3.5 transition-transform ${isLedgerExpanded ? "rotate-180" : ""}`}
                  strokeWidth={2.1}
                />
              </button>
            </div>

            {!isLedgerExpanded ? (
              <p className="text-[#888888] text-xs mt-3">
                {isUz ? "Jurnal yopilgan." : "Лог скрыт."}
              </p>
            ) : isLoadingHistory ? (
              <p className="text-[#888888] text-xs">
                {isUz ? "Yuklanmoqda..." : "Загрузка..."}
              </p>
            ) : historyError ? (
              <p className="text-[#fca5a5] text-xs leading-relaxed mt-3">{historyError}</p>
            ) : historyItems.length === 0 ? (
              <p className="text-[#888888] text-xs">
                {isUz ? "Hozircha operatsiyalar yo'q." : "Операций пока нет."}
              </p>
            ) : (
              <div className="space-y-2 mt-3">
                <div className="flex gap-2 overflow-x-auto pb-1">
                  {availableHistoryMonths.map((monthKey) => {
                    const isActive = activeHistoryMonth === monthKey;
                    return (
                      <button
                        key={monthKey}
                        type="button"
                        onClick={() => setActiveHistoryMonth(monthKey)}
                        className={`shrink-0 rounded-lg border px-3 py-1.5 text-[11px] font-bold uppercase tracking-wide transition-colors ${
                          isActive
                            ? "bg-[#F08800]/20 border-[#F08800] text-[#F8B24E]"
                            : "bg-[#121212] border-[#2a2a2a] text-[#9b9b9b]"
                        }`}
                      >
                        {formatMonthKey(monthKey, language)}
                      </button>
                    );
                  })}
                </div>

                {visibleHistoryItems.map((item) => {
                  const isIncome = Number(item.delta || 0) >= 0;
                  const amountValue = Math.abs(Number(item.delta || 0));
                  return (
                    <div
                      key={item.id}
                      className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3"
                    >
                      <div className="flex items-start justify-between gap-2">
                        <p className="text-[#FCFCFC] text-sm font-bold leading-snug">
                          {buildHistoryTitle(item, language)}
                        </p>
                        <p
                          className={`text-sm font-black whitespace-nowrap ${
                            isIncome ? "text-[#86efac]" : "text-[#fca5a5]"
                          }`}
                        >
                          {isIncome ? "+" : "-"}
                          {formatMoney(amountValue)} UZS
                        </p>
                      </div>
                      <p className="text-[#a3a3a3] text-xs mt-1.5 leading-relaxed">
                        {buildHistoryDetails(item, language)}
                      </p>
                      <div className="mt-2 flex items-center justify-between gap-2 text-[11px] text-[#7a7a7a]">
                        <span>{formatDateTime(Number(item.createdAt || 0), language)}</span>
                        <span>
                          {formatMoney(Number(item.before || 0))} → {formatMoney(Number(item.after || 0))} UZS
                        </span>
                      </div>
                    </div>
                  );
                })}

                {hasMoreHistoryItems && (
                  <button
                    type="button"
                    onClick={() => setVisibleHistoryCount((prev) => prev + 8)}
                    className="w-full rounded-lg border border-[#2f2f2f] bg-[#121212] py-2.5 text-[#d4d4d4] text-xs font-bold uppercase tracking-wide"
                  >
                    {isUz ? "Yana ko'rsatish" : "Показать ещё"}
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
