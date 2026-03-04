import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, Clock3, KeyRound, Loader2, PlusCircle, Server, ShieldCheck, UserRound, WalletCards } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { PageTransition } from "../components/PageTransition";
import { useLanguage } from "../i18n/LanguageContext";
import {
  changePrivilegePassword,
  claimWelcomeBonus,
  fetchServers,
  fetchUserBalance,
  fetchUserBalanceHistory,
  submitLegacyPrivilegeImport,
  verifyPrivilegePassword,
  fetchWelcomeBonusStatus,
  type BalanceHistoryItem,
  type LiveServer,
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

type WelcomeBonusStatus = {
  eligible: boolean;
  claimed: boolean;
  claimedAt: number;
  amount: number;
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

function isValidPrivilegePassword(rawValue: string): boolean {
  return /^[A-Za-z0-9]{1,20}$/.test(String(rawValue || "").trim());
}

function buildHistoryTitle(item: BalanceHistoryItem, language: "ru" | "uz"): string {
  const isUz = language === "uz";
  const meta = item.meta ?? {};
  const productType = getMetaString(meta, "product_type").toLowerCase();

  if (item.type === "welcome_bonus") {
    return isUz ? "Start bonusi olindi" : "Получен стартовый бонус";
  }
  if (item.type === "legacy_import") {
    return isUz ? "Legacy import: imtiyoz qo'shildi" : "Legacy import: привилегия добавлена";
  }
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

  if (item.type === "welcome_bonus") {
    return isUz
      ? "Start bonusi bir martalik tarzda balansga qo'shildi."
      : "Разовый стартовый бонус зачислен на баланс.";
  }

  if (item.type === "legacy_import") {
    const parts: string[] = [];
    const isPermanent = Boolean(meta.is_permanent);
    if (serverName) {
      parts.push(`${isUz ? "Server" : "Сервер"}: ${serverName}`);
    }
    if (privilege) {
      parts.push(`${isUz ? "Imtiyoz" : "Привилегия"}: ${privilege}`);
    }
    if (isPermanent) {
      parts.push(isUz ? "Muddat: doimiy" : "Срок: постоянная");
    } else if (durationMonths > 0) {
      parts.push(`${isUz ? "Import" : "Импорт"}: ${durationMonths} ${isUz ? "oy" : "мес."}`);
    }
    if (parts.length > 0) {
      return parts.join(" • ");
    }
    return isUz ? "Mavjud imtiyoz profilingizga biriktirildi." : "Существующая привилегия привязана к профилю.";
  }

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
  const [welcomeBonusStatus, setWelcomeBonusStatus] = useState<WelcomeBonusStatus | null>(null);
  const [isLoadingWelcomeBonus, setIsLoadingWelcomeBonus] = useState(false);
  const [isClaimingWelcomeBonus, setIsClaimingWelcomeBonus] = useState(false);
  const [welcomeBonusError, setWelcomeBonusError] = useState<string | null>(null);
  const [welcomeBonusToast, setWelcomeBonusToast] = useState<{
    amount: number;
    balanceAfter: number;
  } | null>(null);
  const [topUpToast, setTopUpToast] = useState<{
    amount: number;
    balanceAfter: number;
  } | null>(null);
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
  const [legacyServers, setLegacyServers] = useState<LiveServer[]>([]);
  const [isLoadingLegacyServers, setIsLoadingLegacyServers] = useState(false);
  const [isLegacyModalOpen, setIsLegacyModalOpen] = useState(false);
  const [legacyStep, setLegacyStep] = useState<1 | 2 | 3>(1);
  const [legacyServerId, setLegacyServerId] = useState("");
  const [legacyIdentifierType, setLegacyIdentifierType] = useState<"nickname" | "steam">("nickname");
  const [legacyNickname, setLegacyNickname] = useState("");
  const [legacySteamId, setLegacySteamId] = useState("");
  const [legacyPassword, setLegacyPassword] = useState("");
  const [legacyError, setLegacyError] = useState<string | null>(null);
  const [isSubmittingLegacy, setIsSubmittingLegacy] = useState(false);
  const [legacyImportToast, setLegacyImportToast] = useState<{
    title: string;
    details: string;
  } | null>(null);
  const [isPasswordModalOpen, setIsPasswordModalOpen] = useState(false);
  const [passwordTargetItem, setPasswordTargetItem] = useState<UserPrivilegeItem | null>(null);
  const [currentPrivilegePassword, setCurrentPrivilegePassword] = useState("");
  const [newPrivilegePassword, setNewPrivilegePassword] = useState("");
  const [passwordChangeError, setPasswordChangeError] = useState<string | null>(null);
  const [isVerifyingCurrentPassword, setIsVerifyingCurrentPassword] = useState(false);
  const [isCurrentPasswordVerified, setIsCurrentPasswordVerified] = useState(false);
  const [isSubmittingPasswordChange, setIsSubmittingPasswordChange] = useState(false);
  const [passwordChangeToast, setPasswordChangeToast] = useState<{
    title: string;
    details: string;
  } | null>(null);
  const [highlightPrivilegeId, setHighlightPrivilegeId] = useState("");
  const activePrivilegesRef = useRef<HTMLDivElement | null>(null);
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

  const selectedLegacyServer = useMemo(
    () => legacyServers.find((server) => server.id === legacyServerId) ?? null,
    [legacyServerId, legacyServers],
  );

  const resetLegacyImportState = useCallback(() => {
    setLegacyStep(1);
    setLegacyServerId("");
    setLegacyIdentifierType("nickname");
    setLegacyNickname("");
    setLegacySteamId("");
    setLegacyPassword("");
    setLegacyError(null);
    setIsSubmittingLegacy(false);
  }, []);

  const loadLegacyServers = useCallback(async () => {
    setIsLoadingLegacyServers(true);
    try {
      const servers = await fetchServers();
      setLegacyServers(servers);
      setLegacyError(null);
    } catch {
      setLegacyError(
        isUz
          ? "Serverlar ro'yxatini yuklab bo'lmadi."
          : "Не удалось загрузить список серверов.",
      );
    } finally {
      setIsLoadingLegacyServers(false);
    }
  }, [isUz]);

  const openLegacyImportModal = useCallback(() => {
    resetLegacyImportState();
    setIsLegacyModalOpen(true);
    void loadLegacyServers();
  }, [loadLegacyServers, resetLegacyImportState]);

  const closeLegacyImportModal = useCallback(() => {
    if (isSubmittingLegacy) {
      return;
    }
    setIsLegacyModalOpen(false);
  }, [isSubmittingLegacy]);

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

  const loadWelcomeBonusStatus = useCallback(async (withLoader: boolean) => {
    if (telegramUserId <= 0) {
      setWelcomeBonusStatus(null);
      return;
    }
    if (withLoader) {
      setIsLoadingWelcomeBonus(true);
    }
    try {
      const response = await fetchWelcomeBonusStatus(telegramUserId);
      setWelcomeBonusStatus({
        eligible: Boolean(response.status?.eligible),
        claimed: Boolean(response.status?.claimed),
        claimedAt: Math.max(0, Number(response.status?.claimedAt || 0)),
        amount: Math.max(0, Number(response.status?.amount ?? response.bonusAmount ?? 0)),
      });
      setWelcomeBonusError(null);
    } catch {
      setWelcomeBonusError(
        isUz
          ? "Start bonusi holatini yuklab bo'lmadi."
          : "Не удалось загрузить статус стартового бонуса.",
      );
    } finally {
      if (withLoader) {
        setIsLoadingWelcomeBonus(false);
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

  const handleClaimWelcomeBonus = useCallback(async () => {
    if (telegramUserId <= 0 || isClaimingWelcomeBonus) {
      return;
    }
    setIsClaimingWelcomeBonus(true);
    setWelcomeBonusError(null);

    try {
      const requestId = (
        typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
          ? crypto.randomUUID()
          : `${telegramUserId}-${Date.now()}`
      );
      const response = await claimWelcomeBonus({
        userId: telegramUserId,
        username: user?.username,
        firstName: user?.first_name,
        lastName: user?.last_name,
        requestId,
        language,
      });

      setWelcomeBonusStatus({
        eligible: false,
        claimed: true,
        claimedAt: Math.max(0, Number(response.claim?.claimedAt || 0)),
        amount: Math.max(0, Number(response.claim?.amount ?? response.bonusAmount ?? 0)),
      });
      setBalance(Math.max(0, Number(response.balanceAfter || 0)));
      if (Boolean(response.claimed)) {
        setWelcomeBonusToast({
          amount: Math.max(0, Number(response.claim?.amount ?? response.bonusAmount ?? 0)),
          balanceAfter: Math.max(0, Number(response.balanceAfter || 0)),
        });
      }
      void loadHistory(false);
    } catch {
      setWelcomeBonusError(
        isUz
          ? "Start bonusini olishda xatolik yuz berdi. Qayta urinib ko'ring."
          : "Не удалось получить стартовый бонус. Попробуйте снова.",
      );
      void loadWelcomeBonusStatus(false);
    } finally {
      setIsClaimingWelcomeBonus(false);
    }
  }, [isClaimingWelcomeBonus, isUz, language, loadHistory, loadWelcomeBonusStatus, telegramUserId, user?.first_name, user?.last_name, user?.username]);

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

  const goLegacyNextStep = useCallback(() => {
    setLegacyError(null);
    if (legacyStep === 1) {
      if (!legacyServerId) {
        setLegacyError(isUz ? "Serverni tanlang." : "Выберите сервер.");
        return;
      }
      setLegacyStep(2);
      return;
    }

    if (legacyStep === 2) {
      if (legacyIdentifierType === "steam") {
        if (!legacySteamId.trim()) {
          setLegacyError(isUz ? "STEAM_ID kiriting." : "Введите STEAM_ID.");
          return;
        }
      } else if (!legacyNickname.trim()) {
        setLegacyError(isUz ? "Nick kiriting." : "Введите Nick.");
        return;
      }
      setLegacyStep(3);
    }
  }, [
    isUz,
    legacyIdentifierType,
    legacyNickname,
    legacyServerId,
    legacySteamId,
    legacyStep,
  ]);

  const goLegacyPrevStep = useCallback(() => {
    setLegacyError(null);
    setLegacyStep((prev) => {
      if (prev <= 1) {
        return 1;
      }
      return (prev - 1) as 1 | 2 | 3;
    });
  }, []);

  const handleLegacyImportSubmit = useCallback(async () => {
    if (telegramUserId <= 0) {
      setLegacyError(
        isUz
          ? "Telegram foydalanuvchisini aniqlab bo'lmadi."
          : "Не удалось определить Telegram пользователя.",
      );
      return;
    }
    if (!legacyServerId) {
      setLegacyError(isUz ? "Serverni tanlang." : "Выберите сервер.");
      return;
    }
    if (legacyIdentifierType === "steam") {
      if (!legacySteamId.trim()) {
        setLegacyError(isUz ? "STEAM_ID kiriting." : "Введите STEAM_ID.");
        return;
      }
    } else {
      if (!legacyNickname.trim()) {
        setLegacyError(isUz ? "Nick kiriting." : "Введите Nick.");
        return;
      }
    }
    if (!legacyPassword.trim()) {
      setLegacyError(isUz ? "Parolni kiriting." : "Введите пароль.");
      return;
    }

    setIsSubmittingLegacy(true);
    setLegacyError(null);
    try {
      const response = await submitLegacyPrivilegeImport({
        userId: telegramUserId,
        username: user?.username,
        firstName: user?.first_name,
        lastName: user?.last_name,
        serverId: legacyServerId,
        serverName: selectedLegacyServer?.name ?? "",
        identifierType: legacyIdentifierType,
        nickname: legacyIdentifierType === "nickname" ? legacyNickname.trim() : "",
        steamId: legacyIdentifierType === "steam" ? legacySteamId.trim().toUpperCase() : "",
        password: legacyPassword.trim(),
        language,
      });

      const importedItem = response.privilegeItem;
      if (importedItem) {
        setPrivilegeItems((current) => {
          const withoutDuplicate = current.filter((item) => item.id !== importedItem.id);
          return [importedItem, ...withoutDuplicate].sort(
            (left, right) => Number(right.createdAt || 0) - Number(left.createdAt || 0),
          );
        });
        setHighlightPrivilegeId(importedItem.id);
      }

      await Promise.all([loadPrivileges(false), loadHistory(false)]);
      setIsPrivilegesExpanded(true);
      window.setTimeout(() => {
        activePrivilegesRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 120);
      if (importedItem?.id) {
        window.setTimeout(() => {
          setHighlightPrivilegeId("");
        }, 4500);
      }

      const toastTitle = response.alreadyImported
        ? (isUz ? "Imtiyoz allaqachon bog'langan" : "Привилегия уже привязана")
        : (isUz ? "Imtiyoz profilga qo'shildi" : "Привилегия добавлена в профиль");
      const importedServer = response.imported?.serverName || selectedLegacyServer?.name || "-";
      const importedPrivilege = response.imported?.privilege || "-";
      const reportHint = response.reportSent
        ? (isUz ? "Admin guruhiga hisobot yuborildi." : "Отчёт в админ-группу отправлен.")
        : (isUz ? "Admin guruhiga hisobot yuborilmadi." : "Отчёт в админ-группу не отправлен.");
      setLegacyImportToast({
        title: toastTitle,
        details: `${importedPrivilege} • ${importedServer}. ${reportHint}`,
      });
      setIsLegacyModalOpen(false);
      resetLegacyImportState();
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      setLegacyError(
        message || (isUz ? "Importda xatolik yuz berdi." : "Не удалось импортировать привилегию."),
      );
    } finally {
      setIsSubmittingLegacy(false);
    }
  }, [
    isUz,
    language,
    legacyIdentifierType,
    legacyNickname,
    legacyPassword,
    legacyServerId,
    legacySteamId,
    loadHistory,
    loadPrivileges,
    resetLegacyImportState,
    selectedLegacyServer?.name,
    telegramUserId,
    user?.first_name,
    user?.last_name,
    user?.username,
  ]);

  useEffect(() => {
    void loadBalance(true);
    void loadWelcomeBonusStatus(true);
    void loadHistory(true);
    void loadPrivileges(true);
  }, [loadBalance, loadHistory, loadPrivileges, loadWelcomeBonusStatus]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void loadPrivileges(false);
    }, 60000);
    return () => window.clearInterval(timer);
  }, [loadPrivileges]);

  useEffect(() => {
    const onTopUpSuccess = (event: Event) => {
      const customEvent = event as CustomEvent<{
        creditedAmount?: number;
        balanceAfter?: number;
      }>;
      const creditedAmount = Math.max(0, Number(customEvent.detail?.creditedAmount || 0));
      const balanceAfter = Math.max(0, Number(customEvent.detail?.balanceAfter || 0));
      if (creditedAmount > 0) {
        setTopUpToast({
          amount: creditedAmount,
          balanceAfter,
        });
      }
      void loadBalance(false);
      void loadWelcomeBonusStatus(false);
      void loadHistory(false);
      void loadPrivileges(false);
    };
    window.addEventListener("strike:balance-topup-success", onTopUpSuccess);
    return () => {
      window.removeEventListener("strike:balance-topup-success", onTopUpSuccess);
    };
  }, [loadBalance, loadHistory, loadPrivileges, loadWelcomeBonusStatus]);

  useEffect(() => {
    if (!topUpToast) {
      return undefined;
    }
    const timerId = window.setTimeout(() => {
      setTopUpToast(null);
    }, 4200);
    return () => window.clearTimeout(timerId);
  }, [topUpToast]);

  useEffect(() => {
    if (!welcomeBonusToast) {
      return undefined;
    }
    const timerId = window.setTimeout(() => {
      setWelcomeBonusToast(null);
    }, 4200);
    return () => window.clearTimeout(timerId);
  }, [welcomeBonusToast]);

  useEffect(() => {
    if (!legacyImportToast) {
      return undefined;
    }
    const timerId = window.setTimeout(() => {
      setLegacyImportToast(null);
    }, 4200);
    return () => window.clearTimeout(timerId);
  }, [legacyImportToast]);

  useEffect(() => {
    if (!passwordChangeToast) {
      return undefined;
    }
    const timerId = window.setTimeout(() => {
      setPasswordChangeToast(null);
    }, 4200);
    return () => window.clearTimeout(timerId);
  }, [passwordChangeToast]);

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
  const welcomeBonusAmount = Math.max(0, Number(welcomeBonusStatus?.amount || 10000));
  const shouldShowWelcomeBonusCard = isLoadingWelcomeBonus || !Boolean(welcomeBonusStatus?.claimed);

  const closePasswordChangeModal = useCallback(() => {
    if (isSubmittingPasswordChange || isVerifyingCurrentPassword) {
      return;
    }
    setIsPasswordModalOpen(false);
    setPasswordTargetItem(null);
    setCurrentPrivilegePassword("");
    setNewPrivilegePassword("");
    setIsCurrentPasswordVerified(false);
    setPasswordChangeError(null);
  }, [isSubmittingPasswordChange, isVerifyingCurrentPassword]);

  const openPasswordChangeModal = useCallback((item: UserPrivilegeItem) => {
    const canChangePassword = Boolean(
      item.canChangePassword ??
      (item.identifierType === "nickname" && item.serverId && item.nickname),
    );
    if (!canChangePassword) {
      return;
    }
    setPasswordTargetItem(item);
    setCurrentPrivilegePassword("");
    setNewPrivilegePassword("");
    setIsCurrentPasswordVerified(false);
    setPasswordChangeError(null);
    setIsPasswordModalOpen(true);
  }, []);

  const handleVerifyCurrentPassword = useCallback(async () => {
    if (!passwordTargetItem) {
      setPasswordChangeError(
        isUz
          ? "Faol imtiyoz topilmadi."
          : "Активная привилегия не найдена.",
      );
      return;
    }
    const currentPasswordSafe = currentPrivilegePassword.trim();
    if (!isValidPrivilegePassword(currentPasswordSafe)) {
      setPasswordChangeError(
        isUz
          ? "Joriy parol noto'g'ri formatda (A-Z, a-z, 0-9, 1-20)."
          : "Неверный формат текущего пароля (A-Z, a-z, 0-9, 1-20).",
      );
      return;
    }

    setIsVerifyingCurrentPassword(true);
    setPasswordChangeError(null);
    try {
      const verification = await verifyPrivilegePassword(
        passwordTargetItem.serverId,
        passwordTargetItem.nickname,
        currentPasswordSafe,
        passwordTargetItem.serverName,
      );
      if (!verification.valid) {
        setIsCurrentPasswordVerified(false);
        setPasswordChangeError(
          isUz
            ? "Joriy parol noto'g'ri."
            : "Текущий пароль неверный.",
        );
        return;
      }
      setIsCurrentPasswordVerified(true);
      setPasswordChangeError(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      setIsCurrentPasswordVerified(false);
      setPasswordChangeError(
        message || (isUz ? "Parolni tekshirib bo'lmadi." : "Не удалось проверить пароль."),
      );
    } finally {
      setIsVerifyingCurrentPassword(false);
    }
  }, [currentPrivilegePassword, isUz, passwordTargetItem]);

  const handlePrivilegePasswordChangeSubmit = useCallback(async () => {
    if (telegramUserId <= 0) {
      setPasswordChangeError(
        isUz
          ? "Telegram foydalanuvchisini aniqlab bo'lmadi."
          : "Не удалось определить Telegram пользователя.",
      );
      return;
    }
    if (!passwordTargetItem) {
      setPasswordChangeError(
        isUz
          ? "Faol imtiyoz topilmadi."
          : "Активная привилегия не найдена.",
      );
      return;
    }

    const canChangePassword = Boolean(
      passwordTargetItem.canChangePassword ??
      (passwordTargetItem.identifierType === "nickname" && passwordTargetItem.serverId && passwordTargetItem.nickname),
    );
    if (!canChangePassword) {
      setPasswordChangeError(
        isUz
          ? "Bu imtiyoz uchun parolni almashtirib bo'lmaydi."
          : "Для этой привилегии смена пароля недоступна.",
      );
      return;
    }

    const currentPasswordSafe = currentPrivilegePassword.trim();
    const newPasswordSafe = newPrivilegePassword.trim();
    if (!isValidPrivilegePassword(currentPasswordSafe)) {
      setPasswordChangeError(
        isUz
          ? "Joriy parol noto'g'ri formatda (A-Z, a-z, 0-9, 1-20)."
          : "Неверный формат текущего пароля (A-Z, a-z, 0-9, 1-20).",
      );
      return;
    }
    if (!isValidPrivilegePassword(newPasswordSafe)) {
      setPasswordChangeError(
        isUz
          ? "Yangi parol noto'g'ri formatda (A-Z, a-z, 0-9, 1-20)."
          : "Неверный формат нового пароля (A-Z, a-z, 0-9, 1-20).",
      );
      return;
    }
    if (newPasswordSafe === currentPasswordSafe) {
      setPasswordChangeError(
        isUz
          ? "Yangi parol joriy paroldan farq qilishi kerak."
          : "Новый пароль должен отличаться от текущего.",
      );
      return;
    }
    if (!isCurrentPasswordVerified) {
      setPasswordChangeError(
        isUz
          ? "Avval joriy parolni tekshiring."
          : "Сначала подтвердите текущий пароль.",
      );
      return;
    }

    setIsSubmittingPasswordChange(true);
    setPasswordChangeError(null);
    try {
      const response = await changePrivilegePassword({
        userId: telegramUserId,
        username: user?.username,
        firstName: user?.first_name,
        lastName: user?.last_name,
        serverId: passwordTargetItem.serverId,
        serverName: passwordTargetItem.serverName,
        identifierType: "nickname",
        nickname: passwordTargetItem.nickname,
        currentPassword: currentPasswordSafe,
        newPassword: newPasswordSafe,
        language,
      });

      const nowSeconds = Math.floor(Date.now() / 1000);
      const fallbackItem: UserPrivilegeItem = {
        ...passwordTargetItem,
        password: newPasswordSafe,
        canChangePassword: true,
        lastPasswordChangedAt: Number(response.passwordChangedAt || 0),
        nextPasswordChangeAt: Number(response.nextAllowedAt || 0),
        passwordChangeCooldownSeconds: Number(response.cooldownSeconds || 0),
        passwordChangeSecondsRemaining: Math.max(
          Number(response.nextAllowedAt || 0) - nowSeconds,
          0,
        ),
      };
      const updatedItem = response.privilegeItem ?? fallbackItem;

      setPrivilegeItems((current) => current.map((item) => (
        item.id === passwordTargetItem.id
          ? updatedItem
          : item
      )));

      const nextAllowedAt = Number(response.nextAllowedAt || 0);
      const nextAllowedText = nextAllowedAt > 0
        ? formatDateTime(nextAllowedAt, language)
        : "-";
      setPasswordChangeToast({
        title: isUz ? "Parol yangilandi" : "Пароль обновлён",
        details: isUz
          ? `Keyingi almashtirish: ${nextAllowedText}`
          : `Следующая смена: ${nextAllowedText}`,
      });

      setIsPasswordModalOpen(false);
      setPasswordTargetItem(null);
      setCurrentPrivilegePassword("");
      setNewPrivilegePassword("");
      setIsCurrentPasswordVerified(false);
      setPasswordChangeError(null);
      await loadPrivileges(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      setPasswordChangeError(
        message || (isUz ? "Parolni almashtirib bo'lmadi." : "Не удалось сменить пароль."),
      );
    } finally {
      setIsSubmittingPasswordChange(false);
    }
  }, [
    currentPrivilegePassword,
    isUz,
    language,
    loadPrivileges,
    newPrivilegePassword,
    passwordTargetItem,
    isCurrentPasswordVerified,
    telegramUserId,
    user?.first_name,
    user?.last_name,
    user?.username,
  ]);

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
      if (item.password && item.password.trim()) {
        params.set("password", item.password.trim());
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
                  ? "To'ldirish skrinshot bo'yicha tekshiriladi va balansga qo'shiladi."
                  : "Пополнение проверяется по скриншоту и зачисляется на баланс."}
              </p>
            )}
          </div>

          {shouldShowWelcomeBonusCard && (
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h2 className="text-[#FCFCFC] text-base font-black">
                    {isUz ? "Start bonusi" : "Стартовый бонус"}
                  </h2>
                  <p className="text-[#7f7f7f] text-[11px] mt-1">
                    {isUz
                      ? "Bir martalik sovg'a: balansingizga bonus qo'shiladi."
                      : "Разовый подарок: бонус на баланс для первых покупок."}
                  </p>
                </div>
                <span className="rounded-full border border-[#22c55e]/50 bg-[#052e1c] px-2.5 py-1 text-[10px] font-black uppercase tracking-wide text-[#86efac]">
                  +{formatMoney(welcomeBonusAmount)} UZS
                </span>
              </div>

              <button
                type="button"
                onClick={handleClaimWelcomeBonus}
                disabled={isLoadingWelcomeBonus || isClaimingWelcomeBonus || Boolean(welcomeBonusStatus?.claimed)}
                className={`mt-3 w-full rounded-lg border px-3 py-2.5 text-sm font-black uppercase tracking-wide transition-colors ${
                  welcomeBonusStatus?.claimed
                    ? "bg-[#113322] border-[#1d7f4b] text-[#86efac] cursor-default"
                    : "bg-[#121212] border-[#22c55e]/60 text-[#86efac] hover:border-[#22c55e]"
                } ${
                  isLoadingWelcomeBonus || isClaimingWelcomeBonus ? "opacity-70 cursor-wait" : ""
                }`}
              >
                {isLoadingWelcomeBonus
                  ? (isUz ? "Tekshirilmoqda..." : "Проверка...")
                  : isClaimingWelcomeBonus
                    ? (isUz ? "Olinmoqda..." : "Получение...")
                    : welcomeBonusStatus?.claimed
                      ? (isUz ? "Bonus olingan" : "Бонус получен")
                      : (isUz ? "Bonusni olish" : "Получить бонус")}
              </button>

              <p className="text-[#888888] text-xs leading-relaxed mt-3">
                {isUz
                  ? "Tugmani bosing va bonusni darhol balansga oling."
                  : "Нажмите кнопку и заберите бонус на баланс."}
              </p>

              {welcomeBonusError ? (
                <p className="text-[#fca5a5] text-xs leading-relaxed mt-2">{welcomeBonusError}</p>
              ) : null}
            </div>
          )}

          <div ref={activePrivilegesRef} className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <div className="flex items-start justify-between gap-3">
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
              <div className="flex flex-col items-end gap-2">
                <button
                  type="button"
                  onClick={openLegacyImportModal}
                  className="inline-flex items-center gap-1 rounded-lg border border-[#22c55e]/55 bg-[#0e1d14] px-2 py-1.5 text-[10px] font-black uppercase tracking-wide text-[#86efac] hover:border-[#22c55e] transition-colors"
                >
                  <ShieldCheck className="w-3.5 h-3.5" strokeWidth={2.2} />
                  {isUz ? "Mavjudni qo'shish" : "Добавить существующую"}
                </button>
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
                  const isPermanent = Boolean(item.isPermanent);
                  const safeTotalDays = Math.max(1, Number(item.totalDays || 0));
                  const safeRemainingDays = Math.max(0, Number(item.remainingDays || 0));
                  const progressPercent = Math.max(
                    0,
                    Math.min(100, isPermanent ? 100 : Math.round((safeRemainingDays / safeTotalDays) * 100)),
                  );
                  const isLegacyImported = String(item.source || "").toLowerCase() === "legacy_import";
                  const identifierValue = item.identifierType === "steam"
                    ? item.steamId
                    : item.nickname;
                  const canChangePassword = Boolean(
                    item.canChangePassword ??
                    (item.identifierType === "nickname" && item.serverId && item.nickname),
                  );
                  const nextPasswordChangeAt = Math.max(0, Number(item.nextPasswordChangeAt || 0));
                  const nowSeconds = Math.floor(Date.now() / 1000);
                  const fallbackPasswordChangeSeconds = nextPasswordChangeAt > 0
                    ? Math.max(nextPasswordChangeAt - nowSeconds, 0)
                    : 0;
                  const passwordChangeSecondsRemaining = Math.max(
                    0,
                    Number(item.passwordChangeSecondsRemaining ?? fallbackPasswordChangeSeconds),
                  );
                  const isPasswordCooldownActive = canChangePassword && passwordChangeSecondsRemaining > 0;
                  const passwordChangeInfoText = canChangePassword
                    ? (
                        isPasswordCooldownActive
                          ? (isUz
                            ? `Parolni ${formatDateTime(nextPasswordChangeAt, language)} dan keyin almashtirish mumkin.`
                            : `Пароль можно сменить после ${formatDateTime(nextPasswordChangeAt, language)}.`)
                          : (isUz ? "Parolni hozir almashtirish mumkin." : "Пароль можно сменить сейчас.")
                      )
                    : (
                        item.identifierType === "steam"
                          ? (isUz ? "STEAM_ID rejimida parol ishlatilmaydi." : "В режиме STEAM_ID пароль не используется.")
                          : (isUz ? "Parolni almashtirish hozircha mavjud emas." : "Смена пароля сейчас недоступна.")
                      );
                  return (
                    <div
                      key={item.id}
                      className={`bg-[#121212] border rounded-lg p-3 transition-colors ${
                        highlightPrivilegeId === item.id ? "border-[#22c55e]" : "border-[#2a2a2a]"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <p className="text-[#FCFCFC] text-sm font-black">
                            {item.privilegeLabel}
                          </p>
                          {isLegacyImported && (
                            <span className="rounded-full border border-[#22c55e]/50 bg-[#052e1c] px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-[#86efac]">
                              {isUz ? "Legacy import" : "Legacy import"}
                            </span>
                          )}
                          {isPermanent && (
                            <span className="rounded-full border border-[#60a5fa]/50 bg-[#0c1f39] px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-[#93c5fd]">
                              {isUz ? "Doimiy" : "Постоянная"}
                            </span>
                          )}
                        </div>
                        <p className="text-[#F8B24E] text-xs font-black">
                          {isPermanent ? "∞/∞" : `${safeRemainingDays}/${safeTotalDays}`}
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
                      <p className="text-[#8f8f8f] text-[11px] mt-2 leading-relaxed">{passwordChangeInfoText}</p>
                      <div className="grid grid-cols-2 gap-2 mt-3">
                        <button
                          type="button"
                          disabled={!item.canRenew}
                          onClick={() => handleRenewPrivilege(item)}
                          className={`rounded-lg border px-3 py-2 text-xs font-black uppercase tracking-wide transition-colors ${
                            item.canRenew
                              ? "bg-[#F08800]/12 border-[#F08800]/60 text-[#F8B24E]"
                              : "bg-[#1a1a1a] border-[#2a2a2a] text-[#6a6a6a] cursor-not-allowed"
                          }`}
                        >
                          {isUz ? "Uzatish" : "Продлить"}
                        </button>
                        <button
                          type="button"
                          disabled={!canChangePassword || isPasswordCooldownActive}
                          onClick={() => openPasswordChangeModal(item)}
                          className={`rounded-lg border px-3 py-2 text-xs font-black uppercase tracking-wide transition-colors ${
                            canChangePassword && !isPasswordCooldownActive
                              ? "bg-[#2563eb]/15 border-[#3b82f6]/60 text-[#93c5fd]"
                              : "bg-[#1a1a1a] border-[#2a2a2a] text-[#6a6a6a] cursor-not-allowed"
                          }`}
                        >
                          {item.identifierType === "steam"
                            ? (isUz ? "STEAM mode" : "STEAM режим")
                            : (isUz ? "Parolni almashtirish" : "Сменить пароль")}
                        </button>
                      </div>
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
                  const deltaValue = Number(item.delta || 0);
                  const isIncome = deltaValue > 0;
                  const isNeutral = deltaValue === 0;
                  const amountValue = Math.abs(deltaValue);
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
                            isNeutral
                              ? "text-[#d4d4d4]"
                              : isIncome
                                ? "text-[#86efac]"
                                : "text-[#fca5a5]"
                          }`}
                        >
                          {isNeutral ? "" : isIncome ? "+" : "-"}
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

      {isPasswordModalOpen && (
        <div className="fixed inset-0 z-[131] bg-black/70 backdrop-blur-sm px-3 py-6 overflow-y-auto">
          <div className="max-w-[460px] mx-auto bg-[#121212] border border-[#2a2a2a] rounded-2xl p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="text-[#FCFCFC] text-lg font-black">
                  {isUz ? "Parolni almashtirish" : "Смена пароля"}
                </h3>
                <p className="text-[#8e8e8e] text-xs mt-1">
                  {passwordTargetItem?.serverName || "-"} • {passwordTargetItem?.nickname || "-"}
                </p>
              </div>
              <button
                type="button"
                disabled={isSubmittingPasswordChange || isVerifyingCurrentPassword}
                onClick={closePasswordChangeModal}
                className="rounded-lg border border-[#303030] bg-[#1b1b1b] px-2.5 py-1.5 text-[#a8a8a8] text-xs font-bold uppercase tracking-wide disabled:opacity-60"
              >
                ✕
              </button>
            </div>

            <div className="mt-4 space-y-3">
              <div className="rounded-lg border border-[#2a2a2a] bg-[#161616] p-3 text-[12px] text-[#a5a5a5]">
                <p>
                  {isUz ? "Server" : "Сервер"}:{" "}
                  <span className="text-[#FCFCFC]">{passwordTargetItem?.serverName || "-"}</span>
                </p>
                <p className="mt-1">
                  Nick:{" "}
                  <span className="text-[#FCFCFC]">{passwordTargetItem?.nickname || "-"}</span>
                </p>
                {Number(passwordTargetItem?.nextPasswordChangeAt || 0) > 0 && (
                  <p className="mt-1">
                    {isUz ? "Keyingi almashtirish" : "Следующая смена"}:{" "}
                    <span className="text-[#FCFCFC]">
                      {formatDateTime(Number(passwordTargetItem?.nextPasswordChangeAt || 0), language)}
                    </span>
                  </p>
                )}
              </div>

              <div>
                <p className="text-[#cfcfcf] text-xs mb-1.5">
                  {isUz ? "Joriy parol" : "Текущий пароль"}
                </p>
                <input
                  type="password"
                  value={currentPrivilegePassword}
                  onChange={(event) => {
                    setCurrentPrivilegePassword(event.target.value);
                    setIsCurrentPasswordVerified(false);
                    setPasswordChangeError(null);
                  }}
                  autoComplete="off"
                  spellCheck={false}
                  placeholder={isUz ? "Joriy parol" : "Введите текущий пароль"}
                  className="w-full bg-[#121212] border border-[#2a2a2a] rounded-lg px-3 py-2.5 text-[#FCFCFC] text-sm focus:outline-none focus:border-[#F08800]"
                />
                <div className="mt-2 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={handleVerifyCurrentPassword}
                    disabled={isVerifyingCurrentPassword || isSubmittingPasswordChange}
                    className="rounded-lg border border-[#F08800] bg-[#F08800]/15 px-3 py-2 text-[11px] font-black uppercase tracking-wide text-[#F8B24E] disabled:opacity-60"
                  >
                    {isVerifyingCurrentPassword
                      ? (isUz ? "Tekshirilmoqda..." : "Проверка...")
                      : (isUz ? "Parolni tekshirish" : "Проверить пароль")}
                  </button>
                  {isCurrentPasswordVerified && (
                    <span className="text-[11px] font-bold text-[#86efac]">
                      {isUz ? "Parol tasdiqlandi" : "Пароль подтверждён"}
                    </span>
                  )}
                </div>
              </div>

              {isCurrentPasswordVerified ? (
                <div>
                  <p className="text-[#cfcfcf] text-xs mb-1.5">
                    {isUz ? "Yangi parol" : "Новый пароль"}
                  </p>
                  <input
                    type="password"
                    value={newPrivilegePassword}
                    onChange={(event) => {
                      setNewPrivilegePassword(event.target.value);
                      setPasswordChangeError(null);
                    }}
                    autoComplete="new-password"
                    spellCheck={false}
                    placeholder={isUz ? "Yangi parol (A-Z, a-z, 0-9)" : "Новый пароль (A-Z, a-z, 0-9)"}
                    className="w-full bg-[#121212] border border-[#2a2a2a] rounded-lg px-3 py-2.5 text-[#FCFCFC] text-sm focus:outline-none focus:border-[#F08800]"
                  />
                  <p className="text-[#8f8f8f] text-xs leading-relaxed mt-1.5">
                    {isUz
                      ? "Parol faqat A-Z, a-z, 0-9 va 1-20 belgidan iborat bo'lishi kerak."
                      : "Пароль: только A-Z, a-z, 0-9, длина 1-20 символов."}
                  </p>
                </div>
              ) : (
                <p className="text-[#8f8f8f] text-xs leading-relaxed">
                  {isUz
                    ? "Yangi parolni kiritishdan oldin joriy parolni tekshiring."
                    : "Сначала подтвердите текущий пароль, затем будет доступен ввод нового пароля."}
                </p>
              )}
            </div>

            {passwordChangeError && (
              <div className="mt-4 rounded-lg border border-[#ef4444]/40 bg-[#2d1313] px-3 py-2 text-[#fca5a5] text-xs">
                {passwordChangeError}
              </div>
            )}

            <div className="grid grid-cols-2 gap-2 mt-4">
              <button
                type="button"
                onClick={closePasswordChangeModal}
                disabled={isSubmittingPasswordChange}
                className="rounded-lg border border-[#2a2a2a] bg-[#1b1b1b] py-2.5 text-[#d0d0d0] text-xs font-black uppercase tracking-wide disabled:opacity-60"
              >
                {isUz ? "Bekor qilish" : "Отмена"}
              </button>
              <button
                type="button"
                onClick={handlePrivilegePasswordChangeSubmit}
                disabled={isSubmittingPasswordChange || !isCurrentPasswordVerified}
                className="rounded-lg border border-[#3b82f6] bg-[#3b82f6] py-2.5 text-[#dbeafe] text-xs font-black uppercase tracking-wide disabled:opacity-60 inline-flex items-center justify-center gap-2"
              >
                {isSubmittingPasswordChange && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                {isUz ? "Parolni saqlash" : "Сохранить пароль"}
              </button>
            </div>
          </div>
        </div>
      )}

      {isLegacyModalOpen && (
        <div className="fixed inset-0 z-[130] bg-black/70 backdrop-blur-sm px-3 py-6 overflow-y-auto">
          <div className="max-w-[460px] mx-auto bg-[#121212] border border-[#2a2a2a] rounded-2xl p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="text-[#FCFCFC] text-lg font-black">
                  {isUz ? "Mavjud imtiyozni tasdiqlash" : "Подтвердить существующую привилегию"}
                </h3>
                <p className="text-[#8e8e8e] text-xs mt-1">
                  {isUz ? `Bosqich ${legacyStep} / 3` : `Шаг ${legacyStep} / 3`}
                </p>
              </div>
              <button
                type="button"
                disabled={isSubmittingLegacy}
                onClick={closeLegacyImportModal}
                className="rounded-lg border border-[#303030] bg-[#1b1b1b] px-2.5 py-1.5 text-[#a8a8a8] text-xs font-bold uppercase tracking-wide disabled:opacity-60"
              >
                ✕
              </button>
            </div>

            <div className="grid grid-cols-3 gap-2 mt-4">
              {[1, 2, 3].map((step) => (
                <div
                  key={`legacy-step-${step}`}
                  className={`rounded-lg border px-2 py-1.5 text-center text-[11px] font-bold uppercase tracking-wide ${
                    legacyStep === step
                      ? "bg-[#F08800]/20 border-[#F08800] text-[#F8B24E]"
                      : legacyStep > step
                        ? "bg-[#052e1c] border-[#22c55e]/40 text-[#86efac]"
                        : "bg-[#161616] border-[#2a2a2a] text-[#818181]"
                  }`}
                >
                  {step}
                </div>
              ))}
            </div>

            {legacyStep === 1 && (
              <div className="mt-4 space-y-3">
                <p className="text-[#cfcfcf] text-sm font-semibold flex items-center gap-2">
                  <Server className="w-4 h-4 text-[#F8B24E]" /> {isUz ? "Serverni tanlang" : "Выберите сервер"}
                </p>
                {isLoadingLegacyServers ? (
                  <div className="rounded-lg border border-[#2a2a2a] bg-[#161616] p-4 text-[#9f9f9f] text-sm flex items-center gap-2">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    {isUz ? "Serverlar yuklanmoqda..." : "Загрузка серверов..."}
                  </div>
                ) : legacyServers.length === 0 ? (
                  <div className="rounded-lg border border-[#2a2a2a] bg-[#161616] p-4 text-[#9f9f9f] text-sm">
                    {isUz ? "Serverlar topilmadi." : "Серверы не найдены."}
                  </div>
                ) : (
                  <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                    {legacyServers.map((server) => (
                      <button
                        key={`legacy-server-${server.id}`}
                        type="button"
                        onClick={() => {
                          setLegacyServerId(server.id);
                          setLegacyError(null);
                        }}
                        className={`w-full rounded-lg border px-3 py-2 text-left transition-colors ${
                          legacyServerId === server.id
                            ? "bg-[#F08800]/15 border-[#F08800] text-[#F8B24E]"
                            : "bg-[#161616] border-[#2a2a2a] text-[#d0d0d0]"
                        }`}
                      >
                        <p className="text-sm font-bold">{server.name}</p>
                        <p className="text-[11px] text-[#9a9a9a] mt-1">
                          ID: {server.id} • {server.players}/{server.maxPlayers}
                        </p>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}

            {legacyStep === 2 && (
              <div className="mt-4 space-y-3">
                <p className="text-[#cfcfcf] text-sm font-semibold flex items-center gap-2">
                  <UserRound className="w-4 h-4 text-[#F8B24E]" />
                  {isUz ? "Nick yoki STEAM_ID kiriting" : "Укажите Nick или STEAM_ID"}
                </p>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setLegacyIdentifierType("nickname");
                      setLegacyError(null);
                    }}
                    className={`rounded-lg border px-3 py-2 text-xs font-black uppercase tracking-wide ${
                      legacyIdentifierType === "nickname"
                        ? "bg-[#F08800]/20 border-[#F08800] text-[#F8B24E]"
                        : "bg-[#161616] border-[#2a2a2a] text-[#b0b0b0]"
                    }`}
                  >
                    Nick
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setLegacyIdentifierType("steam");
                      setLegacyError(null);
                    }}
                    className={`rounded-lg border px-3 py-2 text-xs font-black uppercase tracking-wide ${
                      legacyIdentifierType === "steam"
                        ? "bg-[#F08800]/20 border-[#F08800] text-[#F8B24E]"
                        : "bg-[#161616] border-[#2a2a2a] text-[#b0b0b0]"
                    }`}
                  >
                    STEAM_ID
                  </button>
                </div>
                {legacyIdentifierType === "steam" ? (
                  <input
                    type="text"
                    value={legacySteamId}
                    onChange={(event) => {
                      setLegacySteamId(event.target.value.toUpperCase());
                      setLegacyError(null);
                    }}
                    placeholder="STEAM_0:1:123456"
                    className="w-full bg-[#121212] border border-[#2a2a2a] rounded-lg px-3 py-2.5 text-[#FCFCFC] text-sm focus:outline-none focus:border-[#F08800]"
                  />
                ) : (
                  <input
                    type="text"
                    value={legacyNickname}
                    onChange={(event) => {
                      setLegacyNickname(event.target.value);
                      setLegacyError(null);
                    }}
                    placeholder={isUz ? "Nick kiriting" : "Введите Nick"}
                    className="w-full bg-[#121212] border border-[#2a2a2a] rounded-lg px-3 py-2.5 text-[#FCFCFC] text-sm focus:outline-none focus:border-[#F08800]"
                  />
                )}
              </div>
            )}

            {legacyStep === 3 && (
              <div className="mt-4 space-y-3">
                <p className="text-[#cfcfcf] text-sm font-semibold flex items-center gap-2">
                  <KeyRound className="w-4 h-4 text-[#F8B24E]" />
                  {isUz ? "Parolni tasdiqlang" : "Подтвердите пароль"}
                </p>
                <div className="rounded-lg border border-[#2a2a2a] bg-[#161616] p-3 text-[12px] text-[#a5a5a5]">
                  <p>
                    {isUz ? "Server" : "Сервер"}:{" "}
                    <span className="text-[#FCFCFC]">{selectedLegacyServer?.name || "-"}</span>
                  </p>
                  <p className="mt-1">
                    {legacyIdentifierType === "steam" ? "STEAM_ID" : "Nick"}:{" "}
                    <span className="text-[#FCFCFC]">
                      {legacyIdentifierType === "steam" ? legacySteamId || "-" : legacyNickname || "-"}
                    </span>
                  </p>
                </div>
                <input
                  type="text"
                  value={legacyPassword}
                  onChange={(event) => {
                    setLegacyPassword(event.target.value);
                    setLegacyError(null);
                  }}
                  placeholder={
                    isUz ? "Parolni kiriting" : "Введите пароль"
                  }
                  className="w-full bg-[#121212] border border-[#2a2a2a] rounded-lg px-3 py-2.5 text-[#FCFCFC] text-sm focus:outline-none focus:border-[#F08800]"
                />
                <p className="text-[#8f8f8f] text-xs leading-relaxed">
                  {isUz
                    ? "Parol users.ini dagi yozuv bilan bir xil bo'lishi kerak."
                    : "Пароль должен совпадать с записью в users.ini."}
                </p>
              </div>
            )}

            {legacyError && (
              <div className="mt-4 rounded-lg border border-[#ef4444]/40 bg-[#2d1313] px-3 py-2 text-[#fca5a5] text-xs">
                {legacyError}
              </div>
            )}

            <div className="grid grid-cols-2 gap-2 mt-4">
              <button
                type="button"
                onClick={legacyStep === 1 ? closeLegacyImportModal : goLegacyPrevStep}
                disabled={isSubmittingLegacy}
                className="rounded-lg border border-[#2a2a2a] bg-[#1b1b1b] py-2.5 text-[#d0d0d0] text-xs font-black uppercase tracking-wide disabled:opacity-60"
              >
                {legacyStep === 1
                  ? (isUz ? "Yopish" : "Закрыть")
                  : (isUz ? "Orqaga" : "Назад")}
              </button>
              {legacyStep < 3 ? (
                <button
                  type="button"
                  onClick={goLegacyNextStep}
                  disabled={isSubmittingLegacy}
                  className="rounded-lg border border-[#F08800] bg-[#F08800] py-2.5 text-[#121212] text-xs font-black uppercase tracking-wide disabled:opacity-60"
                >
                  {isUz ? "Davom etish" : "Далее"}
                </button>
              ) : (
                <button
                  type="button"
                  onClick={handleLegacyImportSubmit}
                  disabled={isSubmittingLegacy}
                  className="rounded-lg border border-[#22c55e] bg-[#22c55e] py-2.5 text-[#052e1c] text-xs font-black uppercase tracking-wide disabled:opacity-60 inline-flex items-center justify-center gap-2"
                >
                  {isSubmittingLegacy && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                  {isUz ? "Imtiyozni import qilish" : "Импортировать привилегию"}
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {legacyImportToast && (
        <div className="fixed left-3 right-3 top-20 z-[131] max-w-[460px] mx-auto">
          <div className="rounded-2xl border border-[#22c55e]/70 bg-gradient-to-br from-[#0f2a1b] to-[#0a2014] px-4 py-3 shadow-[0_12px_36px_rgba(34,197,94,0.3)]">
            <p className="text-[#dcfce7] text-sm font-black">{legacyImportToast.title}</p>
            <p className="text-[#9feebf] text-xs mt-1.5 leading-relaxed">{legacyImportToast.details}</p>
          </div>
        </div>
      )}

      {passwordChangeToast && (
        <div className="fixed left-3 right-3 top-20 z-[132] max-w-[460px] mx-auto">
          <div className="rounded-2xl border border-[#3b82f6]/70 bg-gradient-to-br from-[#10203d] to-[#0a172b] px-4 py-3 shadow-[0_12px_36px_rgba(59,130,246,0.3)]">
            <p className="text-[#dbeafe] text-sm font-black">{passwordChangeToast.title}</p>
            <p className="text-[#93c5fd] text-xs mt-1.5 leading-relaxed">{passwordChangeToast.details}</p>
          </div>
        </div>
      )}

      {topUpToast && (
        <div className="fixed left-3 right-3 bottom-[calc(5.3rem+env(safe-area-inset-bottom))] z-[131] max-w-[460px] mx-auto">
          <div className="rounded-2xl border border-[#22c55e]/70 bg-gradient-to-br from-[#0f2a1b] to-[#0a2014] px-4 py-3 shadow-[0_12px_38px_rgba(34,197,94,0.32)]">
            <p className="text-[#bbf7d0] text-xs uppercase tracking-[0.14em] font-black">
              {isUz ? "To'ldirish muvaffaqiyatli" : "Пополнение успешно"}
            </p>
            <p className="text-[#dcfce7] text-sm font-black mt-1">
              {isUz ? "Tabriklaymiz!" : "Поздравляем!"} +{formatMoney(topUpToast.amount)} UZS
            </p>
            <p className="text-[#86efac] text-xs mt-1">
              {isUz
                ? `Joriy balans: ${formatMoney(topUpToast.balanceAfter)} UZS`
                : `Текущий баланс: ${formatMoney(topUpToast.balanceAfter)} UZS`}
            </p>
          </div>
        </div>
      )}

      {welcomeBonusToast && (
        <div
          className={`fixed left-3 right-3 z-[130] max-w-[460px] mx-auto ${
            topUpToast
              ? "bottom-[calc(5.3rem+env(safe-area-inset-bottom)+5.8rem)]"
              : "bottom-[calc(5.3rem+env(safe-area-inset-bottom))]"
          }`}
        >
          <div className="rounded-2xl border border-[#22c55e]/60 bg-gradient-to-br from-[#0f2a1b] to-[#0a2014] px-4 py-3 shadow-[0_10px_35px_rgba(34,197,94,0.25)]">
            <p className="text-[#bbf7d0] text-xs uppercase tracking-[0.14em] font-black">
              {isUz ? "Bonus qo'shildi" : "Бонус зачислен"}
            </p>
            <p className="text-[#dcfce7] text-sm font-black mt-1">
              +{formatMoney(welcomeBonusToast.amount)} UZS
            </p>
            <p className="text-[#86efac] text-xs mt-1">
              {isUz
                ? `Joriy balans: ${formatMoney(welcomeBonusToast.balanceAfter)} UZS`
                : `Текущий баланс: ${formatMoney(welcomeBonusToast.balanceAfter)} UZS`}
            </p>
          </div>
        </div>
      )}
    </PageTransition>
  );
}
