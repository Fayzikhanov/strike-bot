import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
} from "react";
import { useSearchParams } from "react-router-dom";
import {
  Award,
  Check,
  ChevronDown,
  Coins,
  Copy,
  Lock,
  PlusCircle,
  Search,
  Server,
  Upload,
  User,
  WalletCards,
} from "lucide-react";
import { PageTransition } from "../components/PageTransition";
import {
  fetchUserBalance,
  fetchBonusAccount,
  fetchPaymentStatus,
  fetchPrivilegeAccount,
  fetchServers,
  notifyPurchaseConfirmed,
  verifyPrivilegePassword,
  type BonusAccountInfo,
  type LiveServer,
  type PaymentStatusInfo,
  type PrivilegeAccountInfo,
  type PurchaseConfirmedResponse,
} from "../api/strikeApi";
import { privileges, type Privilege } from "../data/privileges";
import strikeMarkLogo from "../assets/strike-mark.png";
import {
  getAllowedPrivilegeIdsForServer,
  getPrivilegeTariffsForServer,
  isPublicServer,
  isPurchasablePrivilegeId,
  serverSupportsBonus,
  serverSupportsPrivilege,
  type PurchasablePrivilegeId,
  type TariffOption,
} from "../lib/purchaseRules";
import { type Language, useLanguage } from "../i18n/LanguageContext";
import { useBalanceTopUp } from "../context/BalanceTopUpContext";

type Step = 1 | 2 | 3 | 4 | 5;
type PaymentStatus = "idle" | "processing" | "success";
type PurchasablePrivilege = Privilege & { id: PurchasablePrivilegeId };
type ProductType = "privilege" | "bonus";
type PrivilegeIdentifierType = "nickname" | "steam";
type BonusTariffId = "bonus-2250" | "bonus-7500" | "bonus-14000";
type PublicStyle1PrivilegeId = "vip" | "prime" | "legend";
type PaymentSessionSnapshot = {
  selectedServer: string;
  selectedPrivilege: string;
  selectedTariffMonths: TariffOption["months"] | null;
  selectedBonusTariffId: BonusTariffId | null;
  privilegeIdentifierType: PrivilegeIdentifierType;
  nickname: string;
  password: string;
  currentPassword: string;
  newPassword: string;
  changePasswordChoice: boolean | null;
  renewalRequested: boolean;
  currentPasswordVerified: boolean;
  steamId: string;
  bonusAccountInfo: BonusAccountInfo | null;
  privilegeAccountInfo: PrivilegeAccountInfo | null;
};
type PaymentUploadSession = {
  sessionId: string;
  userId: number;
  startedAt: number;
  expiresAt: number;
  snapshot: PaymentSessionSnapshot;
};

interface ValidationResult {
  isValid: boolean;
  errors: string[];
}

interface BonusTariff {
  id: BonusTariffId;
  bonusAmount: number;
  price: number;
}

interface PaymentApp {
  id: string;
  name: string;
  short: string;
  deepLink: string;
  androidIntent?: string;
  colorClass: string;
}

const paymentApps: PaymentApp[] = [
  {
    id: "click",
    name: "Click",
    short: "CL",
    deepLink: "click://",
    androidIntent: "intent://#Intent;scheme=click;end",
    colorClass: "bg-[#0E7BFF]",
  },
  {
    id: "payme",
    name: "Payme",
    short: "PM",
    deepLink: "payme://",
    androidIntent: "intent://#Intent;scheme=payme;end",
    colorClass: "bg-[#00B6F0]",
  },
  {
    id: "uzumbank",
    name: "UzumBank",
    short: "UZ",
    deepLink: "uzumbank://",
    androidIntent: "intent://#Intent;scheme=uzumbank;end",
    colorClass: "bg-[#7A2CFF]",
  },
  {
    id: "xazna",
    name: "Xazna",
    short: "XZ",
    deepLink: "xazna://",
    androidIntent: "intent://#Intent;scheme=xazna;end",
    colorClass: "bg-[#22A861]",
  },
  {
    id: "paynet",
    name: "Paynet",
    short: "PN",
    deepLink: "paynet://",
    androidIntent: "intent://#Intent;scheme=paynet;end",
    colorClass: "bg-[#F08800]",
  },
];

const BONUS_TARIFFS: readonly BonusTariff[] = [
  { id: "bonus-2250", bonusAmount: 2250, price: 10000 },
  { id: "bonus-7500", bonusAmount: 7500, price: 30000 },
  { id: "bonus-14000", bonusAmount: 14000, price: 50000 },
];

const STEAM_ID_PATTERN = /^STEAM_[01]:[01]:\d{5,15}$/i;
const PUBLIC_STYLE_1_ONE_MONTH_PRICE_BY_ID: Record<PublicStyle1PrivilegeId, number> = {
  vip: 29000,
  prime: 49000,
  legend: 79000,
};
const PUBLIC_STYLE_1_PRIVILEGE_TIER: Record<PublicStyle1PrivilegeId, number> = {
  vip: 1,
  prime: 2,
  legend: 3,
};
const PUBLIC_STYLE_1_FLAGS_TO_ID: Record<string, PublicStyle1PrivilegeId> = {
  t: "vip",
  pt: "prime",
  pst: "legend",
};
const NICKNAME_ONLY_PRIVILEGE_IDS = new Set<string>([
  "moder",
  "admin",
  "gl-admin",
  "admin-cw",
]);
const PAYMENT_UPLOAD_SESSION_STORAGE_KEY = "strike_purchase_upload_session_v1";
const PAYMENT_UPLOAD_SESSION_TTL_MS = 5 * 60 * 1000;

interface PrivilegePaymentAdjustment {
  finalAmount: number;
  creditAmount: number;
  isUpgradeWithCredit: boolean;
  isDowngradeBlocked: boolean;
  existingPrivilegeId: PublicStyle1PrivilegeId | null;
  targetPrivilegeId: PublicStyle1PrivilegeId | null;
  isExistingActive: boolean;
}

function isPurchasablePrivilege(privilege: Privilege): privilege is PurchasablePrivilege {
  return isPurchasablePrivilegeId(privilege.id);
}

function formatPrice(value: number): string {
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

function formatBalanceMoney(value: number): string {
  return Math.max(0, Math.floor(value || 0)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}

function getPrivilegeCashbackPercent(privilegeId: string): number {
  return privilegeId.trim().toLowerCase() === "legend" ? 10 : 5;
}

function calculateCashbackAmount(amount: number, percent: number): number {
  if (!Number.isFinite(amount) || amount <= 0) {
    return 0;
  }
  return Math.floor((amount * percent) / 100);
}

function triggerSuccessHaptic(): void {
  const haptics = (
    window as Window & {
      Telegram?: {
        WebApp?: {
          HapticFeedback?: {
            notificationOccurred?: (type: "error" | "success" | "warning") => void;
          };
        };
      };
    }
  ).Telegram?.WebApp?.HapticFeedback;
  if (typeof haptics?.notificationOccurred === "function") {
    haptics.notificationOccurred("success");
  }
}

function normalizePublicStyle1PrivilegeId(rawValue: string): PublicStyle1PrivilegeId | null {
  const normalized = rawValue.trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  const byFlags = PUBLIC_STYLE_1_FLAGS_TO_ID[normalized];
  if (byFlags) {
    return byFlags;
  }

  if (normalized === "vip" || normalized.includes("vip")) {
    return "vip";
  }
  if (normalized === "prime" || normalized.includes("prime")) {
    return "prime";
  }
  if (normalized === "legend" || normalized.includes("legend")) {
    return "legend";
  }
  return null;
}

function resolveExistingPublicStyle1PrivilegeId(
  accountInfo: PrivilegeAccountInfo | null,
): PublicStyle1PrivilegeId | null {
  if (!accountInfo?.exists) {
    return null;
  }

  return (
    normalizePublicStyle1PrivilegeId(accountInfo.flags) ??
    normalizePublicStyle1PrivilegeId(accountInfo.privilege)
  );
}

function floorToNearest500(rawAmount: number): number {
  if (!Number.isFinite(rawAmount) || rawAmount <= 0) {
    return 0;
  }
  return Math.floor(rawAmount / 500) * 500;
}

function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result;
      if (typeof result === "string" && result.startsWith("data:")) {
        resolve(result);
        return;
      }
      reject(new Error("Failed to read screenshot file"));
    };
    reader.onerror = () => reject(new Error("Failed to read screenshot file"));
    reader.readAsDataURL(file);
  });
}

function extractReadableErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.trim();
    if (message) {
      return message;
    }
  }

  if (typeof error === "string") {
    const message = error.trim();
    if (message) {
      return message;
    }
  }

  if (error && typeof error === "object") {
    const maybeMessage = (error as { message?: unknown }).message;
    if (typeof maybeMessage === "string") {
      const message = maybeMessage.trim();
      if (message) {
        return message;
      }
    }
  }

  return "";
}

function readPaymentUploadSession(): PaymentUploadSession | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(PAYMENT_UPLOAD_SESSION_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Partial<PaymentUploadSession>;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    const sessionId = String(parsed.sessionId ?? "").trim();
    const userId = Number(parsed.userId ?? 0);
    const startedAt = Number(parsed.startedAt ?? 0);
    const expiresAt = Number(parsed.expiresAt ?? 0);
    const snapshot = parsed.snapshot;
    if (
      !sessionId ||
      !Number.isFinite(userId) ||
      userId <= 0 ||
      !Number.isFinite(startedAt) ||
      startedAt <= 0 ||
      !Number.isFinite(expiresAt) ||
      expiresAt <= startedAt ||
      !snapshot ||
      typeof snapshot !== "object"
    ) {
      return null;
    }
    return {
      sessionId,
      userId,
      startedAt,
      expiresAt,
      snapshot: snapshot as PaymentSessionSnapshot,
    };
  } catch {
    return null;
  }
}

function writePaymentUploadSession(session: PaymentUploadSession): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(PAYMENT_UPLOAD_SESSION_STORAGE_KEY, JSON.stringify(session));
  } catch {
    // ignore localStorage write errors
  }
}

function clearPaymentUploadSession(): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(PAYMENT_UPLOAD_SESSION_STORAGE_KEY);
  } catch {
    // ignore localStorage remove errors
  }
}

function formatCountdown(seconds: number): string {
  const safe = Math.max(Math.floor(seconds), 0);
  const mins = Math.floor(safe / 60);
  const secs = safe % 60;
  return `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

function tariffLabel(months: TariffOption["months"], language: Language): string {
  if (language === "uz") {
    return `${months} oyga`;
  }
  return months === 1 ? "На 1 месяц" : `На ${months} месяца`;
}

function validateNickname(rawValue: string, language: Language): ValidationResult {
  const nickname = rawValue.trim();
  const errors: string[] = [];
  const text = {
    required: language === "uz" ? "Nick kiriting." : "Введите ник.",
    noCyrillic:
      language === "uz"
        ? "Ruscha harflardan foydalanib bo'lmaydi."
        : "Нельзя использовать русские символы.",
    invalidSymbols:
      language === "uz"
        ? "Faqat ingliz harflari, raqamlar va quyidagilar mumkin: _ - ! ^ ~ * ( )"
        : "Разрешены только английские буквы, цифры и символы: _ - ! ^ ~ * ( )",
    length:
      language === "uz"
        ? "Nick uzunligi 1 dan 25 gacha bo'lishi kerak."
        : "Ник должен быть длиной от 1 до 25 символов.",
  };

  if (!nickname) {
    errors.push(text.required);
  }

  if (/[А-Яа-яЁё]/.test(nickname)) {
    errors.push(text.noCyrillic);
  }

  if (nickname && !/^[A-Za-z0-9_\-!^~*()]+$/.test(nickname)) {
    errors.push(text.invalidSymbols);
  }

  if (nickname.length < 1 || nickname.length > 25) {
    errors.push(text.length);
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
}

function validatePassword(rawValue: string, language: Language): ValidationResult {
  const password = rawValue.trim();
  const errors: string[] = [];
  const text = {
    required: language === "uz" ? "Parol kiriting." : "Введите пароль.",
    invalid:
      language === "uz"
        ? "Parol faqat ingliz harflari va raqamlardan iborat bo'lishi kerak."
        : "Пароль может состоять только из английских букв и цифр.",
    length:
      language === "uz"
        ? "Parol uzunligi 1 dan 20 tagacha bo'lishi kerak."
        : "Пароль должен быть длиной от 1 до 20 символов.",
  };

  if (!password) {
    errors.push(text.required);
  }

  if (password && !/^[A-Za-z0-9]+$/.test(password)) {
    errors.push(text.invalid);
  }

  if (password.length < 1 || password.length > 20) {
    errors.push(text.length);
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
}

function normalizeSteamId(rawValue: string): string {
  return rawValue.trim().toUpperCase();
}

function validateSteamId(rawValue: string, language: Language): ValidationResult {
  const steamId = normalizeSteamId(rawValue);
  const errors: string[] = [];
  const text = {
    required: language === "uz" ? "STEAM_ID kiriting." : "Введите STEAM_ID.",
    invalid:
      language === "uz"
        ? "STEAM_ID formati noto'g'ri. Misol: STEAM_1:0:175165079."
        : "Неверный формат STEAM_ID. Пример: STEAM_1:0:175165079.",
  };

  if (!steamId) {
    errors.push(text.required);
  } else if (!STEAM_ID_PATTERN.test(steamId)) {
    errors.push(text.invalid);
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
}

function isPublicStyle1Server(server: LiveServer | null): boolean {
  if (!server) {
    return false;
  }

  const portValue = Number.parseInt(String(server.port ?? server.id ?? "").trim(), 10);
  if (Number.isInteger(portValue) && portValue === 27015) {
    return true;
  }

  return server.name.toLowerCase().includes("public style #1");
}

export function Purchase() {
  const { language } = useLanguage();
  const isUz = language === "uz";
  const [searchParams] = useSearchParams();
  const [currentStep, setCurrentStep] = useState<Step>(1);
  const [selectedServer, setSelectedServer] = useState("");
  const [selectedPrivilege, setSelectedPrivilege] = useState("");
  const [selectedTariffMonths, setSelectedTariffMonths] = useState<TariffOption["months"] | null>(
    null,
  );
  const [selectedBonusTariffId, setSelectedBonusTariffId] = useState<BonusTariffId | null>(null);
  const [isBonusExpanded, setIsBonusExpanded] = useState(false);
  const [expandedPrivilegeId, setExpandedPrivilegeId] = useState<PurchasablePrivilegeId | null>(
    null,
  );
  const [privilegeIdentifierType, setPrivilegeIdentifierType] =
    useState<PrivilegeIdentifierType>("nickname");
  const [nickname, setNickname] = useState("");
  const [password, setPassword] = useState("");
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [changePasswordChoice, setChangePasswordChoice] = useState<boolean | null>(null);
  const [renewalRequested, setRenewalRequested] = useState(false);
  const [currentPasswordVerified, setCurrentPasswordVerified] = useState(false);
  const [steamId, setSteamId] = useState("");
  const [bonusAccountInfo, setBonusAccountInfo] = useState<BonusAccountInfo | null>(null);
  const [privilegeAccountInfo, setPrivilegeAccountInfo] = useState<PrivilegeAccountInfo | null>(null);
  const [screenshot, setScreenshot] = useState<File | null>(null);
  const [screenshotPreviewUrl, setScreenshotPreviewUrl] = useState<string | null>(null);
  const [paymentStatus, setPaymentStatus] = useState<PaymentStatus>("idle");
  const [submissionError, setSubmissionError] = useState<string | null>(null);
  const [isResolvingStep3, setIsResolvingStep3] = useState(false);
  const [isResolvingPrivilegeAccount, setIsResolvingPrivilegeAccount] = useState(false);
  const [isVerifyingCurrentPassword, setIsVerifyingCurrentPassword] = useState(false);
  const [purchaseResponse, setPurchaseResponse] = useState<PurchaseConfirmedResponse | null>(null);
  const [paymentUploadSession, setPaymentUploadSession] = useState<PaymentUploadSession | null>(null);
  const [paymentSessionRemainingSeconds, setPaymentSessionRemainingSeconds] = useState(0);
  const [paymentBanStatus, setPaymentBanStatus] = useState<PaymentStatusInfo | null>(null);
  const [paymentBanSecondsLeft, setPaymentBanSecondsLeft] = useState(0);
  const [paymentBanInfoLoaded, setPaymentBanInfoLoaded] = useState(true);
  const [passwordCopied, setPasswordCopied] = useState(false);
  const [cardCopied, setCardCopied] = useState(false);
  const [servers, setServers] = useState<LiveServer[]>([]);
  const [isLoadingServers, setIsLoadingServers] = useState(true);
  const [serversError, setServersError] = useState<string | null>(null);
  const [userBalance, setUserBalance] = useState(0);
  const [isLoadingBalance, setIsLoadingBalance] = useState(false);
  const [balanceBannerMessage, setBalanceBannerMessage] = useState<string | null>(null);
  const [cashbackToast, setCashbackToast] = useState<{ amount: number; percent: number } | null>(null);
  const { openTopUp } = useBalanceTopUp();
  const purchaseNotificationSentRef = useRef(false);
  const renewPrefillAppliedRef = useRef(false);
  const actionsContainerRef = useRef<HTMLDivElement | null>(null);
  const nicknameStepCardRef = useRef<HTMLDivElement | null>(null);
  const passwordStepCardRef = useRef<HTMLDivElement | null>(null);
  const [keyboardInset, setKeyboardInset] = useState(0);
  const paymentCardNumberRaw = "5614 6822 1666 1316";
  const paymentRecipientName = "Murod Fayzixanov";
  const isAndroidDevice = useMemo(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return /android/i.test(window.navigator.userAgent);
  }, []);
  const isIOSDevice = useMemo(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return /iP(hone|od|ad)/i.test(window.navigator.userAgent);
  }, []);
  const telegramUserId = useMemo(() => {
    if (typeof window === "undefined") {
      return 0;
    }
    const userId = Number(
      (
        window as Window & {
          Telegram?: {
            WebApp?: {
              initDataUnsafe?: {
                user?: {
                  id?: number;
                };
              };
            };
          };
        }
      ).Telegram?.WebApp?.initDataUnsafe?.user?.id ?? 0,
    );
    if (!Number.isFinite(userId) || userId <= 0) {
      return 0;
    }
    return Math.floor(userId);
  }, []);

  const nicknameValidation = useMemo(
    () => validateNickname(nickname, language),
    [nickname, language],
  );
  const passwordValidation = useMemo(
    () => validatePassword(password, language),
    [password, language],
  );
  const currentPasswordValidation = useMemo(
    () => validatePassword(currentPassword, language),
    [currentPassword, language],
  );
  const newPasswordValidation = useMemo(
    () => validatePassword(newPassword, language),
    [newPassword, language],
  );
  const normalizedSteamId = useMemo(() => normalizeSteamId(steamId), [steamId]);
  const steamIdValidation = useMemo(
    () => validateSteamId(steamId, language),
    [steamId, language],
  );

  const t = useMemo(
    () => ({
      step: isUz ? "Bosqich" : "Шаг",
      of: isUz ? "dan" : "из",
      serverSelection: isUz ? "Server tanlash" : "Выбор сервера",
      privilegeSelection: isUz ? "Imtiyozni tanlash" : "Выбор привилегии",
      availableServersForPrivilege: isUz
        ? "Ushbu imtiyoz uchun mavjud serverlar:"
        : "Доступные серверы для привилегии:",
      retry: isUz ? "Qayta urinish" : "Retry",
      noServersForPrivilege: isUz
        ? "Tanlangan imtiyoz uchun mavjud server yo'q."
        : "Для выбранной привилегии нет доступных серверов.",
      noPrivilegesForServer: isUz
        ? "Tanlangan serverda sotib olish uchun imtiyozlar mavjud emas."
        : "На выбранном сервере нет доступных привилегий для покупки.",
      bonusTitle: isUz ? "Bonuslar" : "Бонусы",
      bonusSubtitle: isUz
        ? "Bu imtiyoz emas, hisobingizga bir martalik bonus qo'shiladi."
        : "Это не привилегия, а разовое пополнение бонусов аккаунта.",
      bonusPackage: isUz ? "bonus" : "бонус",
      from: isUz ? "dan" : "от",
      perMonth: isUz ? "oyiga" : "месяц",
      privilegeAuthTitle: isUz ? "Berish usulini tanlang" : "Выберите способ выдачи",
      privilegeAuthNick: isUz ? "NickName + PW" : "NickName + PW",
      privilegeAuthSteam: "STEAM_ID",
      nicknameOnlyPrivilegeRule: isUz
        ? "Ushbu imtiyoz faqat NickName + PW orqali beriladi."
        : "Для этой привилегии доступна только выдача на NickName + PW.",
      enterNick: isUz ? "Nick kiriting" : "Введите ник",
      gameNick: isUz ? "O'yindagi nickingiz" : "Ваш ник в игре",
      nickPlaceholder: isUz ? "Nick kiriting" : "Введите ваш ник",
      nickRules: isUz
        ? [
            "Nick Counter-Strike 1.6 dagi nick bilan mos bo'lishi kerak.",
            "Ruscha harflardan foydalanib bo'lmaydi.",
            "Uzunligi: 1-25 belgi.",
            "Ruxsat etilganlar: ingliz harflari, raqamlar va _ - ! ^ ~ * ( )",
          ]
        : [
            "Ник должен совпадать с ником в Counter-Strike 1.6.",
            "Нельзя использовать русские символы.",
            "Длина ника: от 1 до 25 символов.",
            "Разрешены: английские буквы, цифры и символы _ - ! ^ ~ * ( )",
          ],
      enterSteamId: isUz ? "STEAM_ID kiriting" : "Введите STEAM_ID",
      steamIdLabel: isUz ? "Sizning STEAM_ID" : "Ваш STEAM_ID",
      steamIdPlaceholder: "STEAM_1:0:175165079",
      steamIdRules: isUz
        ? [
            "Faqat to'g'ri STEAM_ID kiriting (masalan: STEAM_1:0:175165079).",
            "Noto'g'ri STEAM_ID kiritsangiz, imtiyoz boshqa o'yinchiga beriladi.",
          ]
        : [
            "Указывайте только точный STEAM_ID (пример: STEAM_1:0:175165079).",
            "Если ошибётесь в STEAM_ID, привилегия уйдёт другому игроку.",
          ],
      steamLookupInProgress: isUz ? "Hisob tekshirilmoqda..." : "Проверяем аккаунт...",
      steamLookupFailed: isUz
        ? "Ushbu STEAM_ID bo'yicha o'yinchi topilmadi."
        : "Игрок с таким STEAM_ID не найден.",
      privilegeLookupInProgress: isUz
        ? "Tizimda tekshirilmoqda..."
        : "Проверяем в системе.",
      privilegeLookupFailed: isUz
        ? "Tizimda tekshirib bo'lmadi. Qayta urinib ko'ring."
        : "Не удалось проверить в системе. Попробуйте ещё раз.",
      existingPrivilegeFound: isUz
        ? "Ushbu identifikator uchun serverda imtiyoz allaqachon mavjud."
        : "На этом сервере уже есть привилегия для этого идентификатора.",
      existingPrivilegeType: isUz ? "Imtiyoz" : "Привилегия",
      selectedPrivilegeType: isUz ? "Tanlangan imtiyoz" : "Выбранная привилегия",
      existingPrivilegeDays: isUz ? "Muddati (kun)" : "Срок (дни)",
      existingPrivilegePermanent: isUz ? "Muddati: cheksiz" : "Срок: бессрочно",
      existingPrivilegeDisabled: isUz ? "Status: o'chirilgan (komment)" : "Статус: отключена (комментарий)",
      permanentPrivilegeBlocked: isUz
        ? "Bu identifikator uchun tizimda cheksiz imtiyoz bor. Boshqa nick yoki STEAM_ID kiriting."
        : "Для этого идентификатора уже выдана бессрочная привилегия. Укажите другой NickName или STEAM_ID.",
      upgradeCreditApplied: isUz
        ? "Qolgan muddat summasi hisobga olindi."
        : "Учтён баланс за оставшиеся дни текущей привилегии.",
      upgradeCreditAmount: isUz ? "Qolgan muddat balansi" : "Баланс за остаток",
      recalculatedAmount: isUz ? "Qayta hisoblangan summa" : "Пересчитанная сумма",
      downgradeBlocked: isUz
        ? "Faol muddat bor paytda qimmat imtiyozdan arzoniga o'tib bo'lmaydi. Yangi nick uchun sotib oling yoki muddat tugashini kuting."
        : "Нельзя перейти с более дорогой привилегии на более дешёвую, пока текущая привилегия активна. Купите на новый ник или дождитесь окончания срока.",
      renewQuestion: isUz
        ? "Ushbu imtiyozni uzaytirmoqchimisiz?"
        : "Хотите продлить эту привилегию?",
      renewYes: isUz ? "Ha, uzaytirish" : "Да, продлить",
      renewNo: isUz ? "Yo'q, boshqa nick" : "Нет, другой ник",
      currentPasswordLabel: isUz ? "Joriy parol" : "Текущий пароль",
      currentPasswordPlaceholder: isUz ? "Hozirgi parolni kiriting" : "Введите текущий пароль",
      currentPasswordCheck: isUz ? "Parolni tekshirish" : "Проверить пароль",
      currentPasswordVerified: isUz ? "Parol tasdiqlandi." : "Текущий пароль подтверждён.",
      currentPasswordInvalid: isUz
        ? "Parol noto'g'ri. users.ini dagi parolni kiriting."
        : "Неверный пароль. Введите пароль, который сейчас стоит в users.ini.",
      changePasswordQuestion: isUz ? "Parolni yangilamoqchimisiz?" : "Хотите поменять пароль?",
      changePasswordYes: isUz ? "Ha, o'zgartiraman" : "Да, хочу",
      changePasswordNo: isUz ? "Yo'q, eskisi qolsin" : "Нет, оставлю старый",
      newPasswordLabel: isUz ? "Yangi parol" : "Новый пароль",
      newPasswordPlaceholder: isUz ? "Yangi parolni kiriting" : "Введите новый пароль",
      renewSelectRequired: isUz
        ? "Davom etish uchun uzaytirishni tasdiqlang."
        : "Чтобы продолжить, подтвердите продление.",
      bonusConfirmation: isUz ? "Bonuslarni tasdiqlash" : "Подтверждение бонусов",
      yourNickname: isUz ? "Sizning nickingiz" : "Ваш ник",
      yourBonuses: isUz ? "Sizning bonuslaringiz" : "Ваши бонусы",
      bonusConfirmQuestionStart: isUz
        ? "Siz haqiqatan ham ushbu akkauntga "
        : "Вы уверены, что хотите зачислить ",
      bonusConfirmQuestionEnd: isUz ? " bonus qo'shmoqchimisiz?" : " бонусов на этот аккаунт?",
      setPassword: isUz ? "Parol o'rnatish" : "Установите пароль",
      nicknameLabel: "Nickname:",
      steamLabel: "STEAM_ID",
      passwordLabel: isUz ? "Imtiyoz uchun parol o'ylab toping" : "Придумайте пароль для привилегии",
      passwordPlaceholder: isUz
        ? "Parol (ingliz harflari va raqamlar)"
        : "Введите пароль (английские буквы и цифры)",
      passwordRules: isUz
        ? [
            "Parol faqat ingliz harflari va raqamlardan iborat bo'lishi kerak.",
            "Parol uzunligi: 1 dan 20 gacha.",
          ]
        : [
            "Пароль может состоять только из английских букв и цифр.",
            "Длина пароля: от 1 до 20 символов.",
          ],
      uploadPaymentProof: isUz ? "To'lov tasdiqini yuklang" : "Загрузите подтверждение оплаты",
      amountToPay: isUz ? "To'lov summasi" : "Сумма к оплате",
      paymentCard: isUz ? "To'lov uchun karta raqami" : "Номер карты для оплаты",
      recipient: isUz ? "Qabul qiluvchi F.I.O:" : "ФИО получателя:",
      transferTextStart: isUz ? "Miniapp'ni yopmasdan " : "Не закрывая миниапп, переведите ",
      transferTextMiddle: isUz
        ? " ni yuqoridagi kartaga o'tkazing. Uzum, Payme, Click, Xazna, Paynet va boshqa ilovalar orqali to'lash mumkin."
        : " на карту выше. Можете оплатить через любое удобное приложение: Uzum, Payme, Click, Xazna, Paynet и другие.",
      transferTextEnd: isUz
        ? " To'lovdan keyin tasdiq skrinshotini quyiga yuboring."
        : " После перевода сделайте скриншот подтверждения и отправьте его ниже.",
      choosePaymentApp: isUz
        ? "Tezkor to'lov uchun ilovani tanlang:"
        : "Выберите приложение для быстрой оплаты:",
      changeFile: isUz ? "Faylni almashtirish uchun bosing" : "Нажмите, чтобы изменить файл",
      uploadScreenshot: isUz ? "Skrinshot yuklash" : "Загрузить скриншот",
      uploadHint: isUz ? "PNG, JPG 10MB gacha" : "PNG, JPG до 10MB",
      uploadHintBottom: isUz
        ? "To'lov tasdiq skrinshotini yuklang"
        : "Загрузите скриншот подтверждения оплаты",
      paymentSessionTimerLabel: isUz ? "To'lov oynasi" : "Окно оплаты",
      paymentSessionLockedTitle: isUz
        ? "Skrinshot yuborish sessiyasi faol"
        : "Сессия отправки скриншота активна",
      paymentSessionLockedHint: isUz
        ? "Taymer tugamaguncha ushbu bosqichdan chiqib bo'lmaydi."
        : "Пока таймер не закончится, выйти из этого шага нельзя.",
      paymentSessionExpired: isUz
        ? "To'lov sessiyasi tugadi. Sahifa yangilanadi."
        : "Сессия оплаты истекла. Страница будет перезагружена.",
      paymentBannedTitle: isUz
        ? "Siz vaqtincha bloklangansiz"
        : "Вы временно заблокированы",
      paymentBannedHint: isUz
        ? "Sabab: noto'g'ri skrinshotlar limiti tugadi."
        : "Причина: исчерпан лимит неверных скриншотов.",
      processing: isUz ? "To'lovingizni tekshiryapmiz" : "Обрабатываем вашу оплату",
      issuedPrivilege: isUz ? "Imtiyoz berildi. Yaxshi o'yin!" : "Привилегия выдана. Приятной игры.",
      issuedBonus: isUz ? "Bonuslar muvaffaqiyatli qo'shildi!" : "Бонусы успешно зачислены!",
      processingText: isUz
        ? "To'lov tasdiqlangach sizga "
        : "Как только оплата удостоверится вам будет выдана ваша ",
      processingTextMiddle: isUz ? " imtiyozi " : " на сервере ",
      processingTextSuffix: isUz ? " serverida, nick: " : ", с вашим ником: ",
      processingTextSteamSuffix: isUz ? " serverida, STEAM_ID: " : ", с вашим STEAM_ID: ",
      processingBonusText: isUz
        ? "To'lov tasdiqlanmoqda va bonuslar hisobingizga qo'shilmoqda..."
        : "Проверяем оплату и начисляем бонусы на ваш аккаунт...",
      serverLabel: isUz ? "Server" : "Сервер",
      nickLabel: "Nick",
      consoleInstruction: isUz
        ? "Imtiyoz bilan serverga kirish uchun konsolda quyidagi kodni yozing:"
        : "Чтобы войти на сервер с привилегией, напишите в консоли следующий код:",
      back: isUz ? "Orqaga" : "Назад",
      next: isUz ? "Keyingi" : "Далее",
      buyPrivilege: isUz ? "Imtiyoz sotib olish" : "Купить привилегию",
      buyBonus: isUz ? "Bonuslarni sotib olish" : "Купить бонусы",
      submitFailed: isUz
        ? "Xatolik yuz berdi. Qayta urinib ko'ring."
        : "Произошла ошибка. Попробуйте ещё раз.",
      pendingPrivilegeNote: isUz
        ? "Imtiyoz xaridi balansdan yechiladi. Balans yetarli bo'lmasa, avval hisobni to'ldiring."
        : "Покупка привилегии списывается с баланса. Если средств не хватает, сначала пополните счёт.",
      pendingBonusNote: isUz
        ? "Bonuslar xaridi balansdan yechiladi va STEAM_ID ga qo'shiladi."
        : "Покупка бонусов списывается с баланса и зачисляется на STEAM_ID.",
      balanceTitle: isUz ? "Balans" : "Баланс",
      topUpBalance: isUz ? "Hisobni to'ldirish" : "Пополнить счёт",
      balanceUpdated: isUz ? "Balans yangilandi." : "Баланс обновлён.",
      balanceLoadFailed: isUz
        ? "Balansni yuklab bo'lmadi."
        : "Не удалось загрузить баланс.",
      insufficientBalanceTitle: isUz ? "Balans yetarli emas" : "Недостаточно средств",
      insufficientBalanceHint: isUz
        ? "Tanlangan xarid uchun mablag' yetarli emas."
        : "Для выбранной покупки на балансе не хватает средств.",
      missingAmountLabel: isUz ? "Yetishmayapti" : "Не хватает",
      enoughBalanceHint: isUz
        ? "Balans yetarli, xaridni tasdiqlashingiz mumkin."
        : "Баланса достаточно, можно подтвердить покупку.",
      cashbackLabel: isUz ? "Keshbek" : "Кэшбек",
      cashbackWillReturn: isUz ? "Xariddan qaytadi" : "Вернётся с покупки",
      cashbackCredited: isUz ? "Keshbek hisobga qaytarildi" : "Кэшбек зачислен на баланс",
      cashbackToastTitle: isUz ? "Keshbek olindi" : "Кэшбек получен",
      requiredAmount: isUz ? "kerakli summa" : "нужную сумму",
      publicServers: isUz ? "Public Serverlar" : "Public Servers",
      mixServers: isUz ? "MIX Serverlar" : "MIX Servers",
      publicDesc: isUz ? "Strike.Uz public serverlari." : "Strike.Uz public servers.",
      mixDesc: isUz ? "Strike.Uz MIX/CW serverlari." : "Strike.Uz MIX/CW servers.",
    }),
    [isUz],
  );

  const requestedPrivilege = useMemo<PurchasablePrivilegeId | null>(() => {
    const rawValue = searchParams.get("privilege");
    if (!rawValue) {
      return null;
    }

    const normalized = rawValue.toLowerCase();
    return isPurchasablePrivilegeId(normalized) ? normalized : null;
  }, [searchParams]);
  const requestedServerId = useMemo(
    () => String(searchParams.get("server") ?? "").trim(),
    [searchParams],
  );
  const requestedRenewFlow = useMemo(
    () => String(searchParams.get("renew") ?? "").trim() === "1",
    [searchParams],
  );
  const requestedIdentifierType = useMemo<PrivilegeIdentifierType>(() => {
    const rawValue = String(searchParams.get("identifierType") ?? "").trim().toLowerCase();
    return rawValue === "steam" ? "steam" : "nickname";
  }, [searchParams]);
  const requestedNickname = useMemo(
    () => String(searchParams.get("nickname") ?? "").trim(),
    [searchParams],
  );
  const requestedSteamId = useMemo(
    () => String(searchParams.get("steamId") ?? "").trim().toUpperCase(),
    [searchParams],
  );
  const requestedPassword = useMemo(
    () => String(searchParams.get("password") ?? "").trim(),
    [searchParams],
  );

  const requestedPrivilegeInfo = useMemo(
    () =>
      requestedPrivilege
        ? privileges.find((privilege) => privilege.id === requestedPrivilege)
        : undefined,
    [requestedPrivilege],
  );

  useEffect(() => {
    setExpandedPrivilegeId(requestedPrivilege);
  }, [requestedPrivilege]);

  useEffect(() => {
    return () => {
      if (screenshotPreviewUrl) {
        URL.revokeObjectURL(screenshotPreviewUrl);
      }
    };
  }, [screenshotPreviewUrl]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const viewport = window.visualViewport;
    if (!viewport) {
      return;
    }

    const updateKeyboardInset = () => {
      const rawInset = Math.max(
        0,
        Math.round(window.innerHeight - viewport.height - viewport.offsetTop),
      );
      setKeyboardInset(rawInset > 48 ? rawInset : 0);
    };

    updateKeyboardInset();
    viewport.addEventListener("resize", updateKeyboardInset);
    viewport.addEventListener("scroll", updateKeyboardInset);
    window.addEventListener("orientationchange", updateKeyboardInset);

    return () => {
      viewport.removeEventListener("resize", updateKeyboardInset);
      viewport.removeEventListener("scroll", updateKeyboardInset);
      window.removeEventListener("orientationchange", updateKeyboardInset);
    };
  }, []);

  const getHeaderOffset = useCallback(() => {
    const header = document.querySelector("header");
    const baseOffset = 88;
    if (!header) {
      return baseOffset;
    }

    const rect = header.getBoundingClientRect();
    return Math.max(baseOffset, Math.round(rect.height) + 10);
  }, []);

  const scrollElementToTop = useCallback((element: HTMLElement | null, delayMs = 0) => {
    const run = () => {
      if (!element) {
        return;
      }

      const rect = element.getBoundingClientRect();
      const currentY = window.scrollY || window.pageYOffset;
      const headerOffset = getHeaderOffset();
      const targetY = Math.max(0, currentY + rect.top - headerOffset);
      window.scrollTo({ top: targetY, behavior: "smooth" });
    };

    if (delayMs > 0) {
      window.setTimeout(run, delayMs);
      return;
    }

    run();
  }, [getHeaderOffset]);

  const scrollActionsToScreenMiddle = useCallback((delayMs = 0) => {
    const run = () => {
      const actions = actionsContainerRef.current;
      if (!actions) {
        return;
      }

      const rect = actions.getBoundingClientRect();
      const currentY = window.scrollY || window.pageYOffset;
      const viewportHeight = window.visualViewport?.height ?? window.innerHeight;
      const targetY = Math.max(
        0,
        currentY + rect.top - Math.max(16, viewportHeight / 2 - rect.height / 2),
      );
      window.scrollTo({ top: targetY, behavior: "smooth" });
    };

    if (delayMs > 0) {
      window.setTimeout(run, delayMs);
      return;
    }

    run();
  }, []);

  const clearUploadSessionState = useCallback((removeFromStorage = true) => {
    setPaymentUploadSession(null);
    setPaymentSessionRemainingSeconds(0);
    if (removeFromStorage) {
      clearPaymentUploadSession();
    }
  }, []);

  const applyPaymentSnapshot = useCallback((snapshot: PaymentSessionSnapshot) => {
    setSelectedServer(snapshot.selectedServer ?? "");
    setSelectedPrivilege(snapshot.selectedPrivilege ?? "");
    setSelectedTariffMonths(snapshot.selectedTariffMonths ?? null);
    setSelectedBonusTariffId(snapshot.selectedBonusTariffId ?? null);
    setPrivilegeIdentifierType(snapshot.privilegeIdentifierType ?? "nickname");
    setNickname(snapshot.nickname ?? "");
    setPassword(snapshot.password ?? "");
    setCurrentPassword(snapshot.currentPassword ?? "");
    setNewPassword(snapshot.newPassword ?? "");
    setChangePasswordChoice(
      typeof snapshot.changePasswordChoice === "boolean" ? snapshot.changePasswordChoice : null,
    );
    setRenewalRequested(Boolean(snapshot.renewalRequested));
    setCurrentPasswordVerified(Boolean(snapshot.currentPasswordVerified));
    setSteamId(snapshot.steamId ?? "");
    setBonusAccountInfo(snapshot.bonusAccountInfo ?? null);
    setPrivilegeAccountInfo(snapshot.privilegeAccountInfo ?? null);
    setExpandedPrivilegeId(
      snapshot.selectedPrivilege && isPurchasablePrivilegeId(snapshot.selectedPrivilege)
        ? snapshot.selectedPrivilege
        : null,
    );
    setCurrentStep(5);
    setPaymentStatus("idle");
    setSubmissionError(null);
    setScreenshot(null);
    setScreenshotPreviewUrl((currentUrl) => {
      if (currentUrl) {
        URL.revokeObjectURL(currentUrl);
      }
      return null;
    });
  }, []);

  const buildPaymentSnapshot = useCallback((): PaymentSessionSnapshot => ({
    selectedServer,
    selectedPrivilege,
    selectedTariffMonths,
    selectedBonusTariffId,
    privilegeIdentifierType,
    nickname,
    password,
    currentPassword,
    newPassword,
    changePasswordChoice,
    renewalRequested,
    currentPasswordVerified,
    steamId,
    bonusAccountInfo,
    privilegeAccountInfo,
  }), [
    bonusAccountInfo,
    changePasswordChoice,
    currentPassword,
    currentPasswordVerified,
    newPassword,
    nickname,
    password,
    privilegeAccountInfo,
    privilegeIdentifierType,
    renewalRequested,
    selectedBonusTariffId,
    selectedPrivilege,
    selectedServer,
    selectedTariffMonths,
    steamId,
  ]);

  const startPaymentUploadSession = useCallback((): PaymentUploadSession | null => {
    if (telegramUserId <= 0) {
      return null;
    }
    const now = Date.now();
    const snapshot = buildPaymentSnapshot();
    const existingSession = paymentUploadSession;
    const isExistingUsable = Boolean(
      existingSession &&
      existingSession.userId === telegramUserId &&
      now < existingSession.expiresAt,
    );

    const nextSession: PaymentUploadSession = isExistingUsable
      ? {
          ...existingSession,
          snapshot,
        }
      : {
          sessionId: `${telegramUserId}-${Math.random().toString(36).slice(2, 10)}-${now}`,
          userId: telegramUserId,
          startedAt: now,
          expiresAt: now + PAYMENT_UPLOAD_SESSION_TTL_MS,
          snapshot,
        };

    setPaymentUploadSession(nextSession);
    setPaymentSessionRemainingSeconds(
      Math.max(Math.ceil((nextSession.expiresAt - now) / 1000), 0),
    );
    writePaymentUploadSession(nextSession);
    return nextSession;
  }, [buildPaymentSnapshot, paymentUploadSession, telegramUserId]);

  useEffect(() => {
    // Legacy screenshot-payment sessions are no longer used in balance flow.
    clearUploadSessionState(true);
  }, [clearUploadSessionState]);

  useEffect(() => {
    if (!paymentUploadSession) {
      setPaymentSessionRemainingSeconds(0);
      return;
    }

    const tick = () => {
      const now = Date.now();
      const remaining = Math.max(Math.ceil((paymentUploadSession.expiresAt - now) / 1000), 0);
      setPaymentSessionRemainingSeconds(remaining);
      if (remaining <= 0) {
        clearUploadSessionState();
        setSubmissionError(t.paymentSessionExpired);
        window.setTimeout(() => window.location.reload(), 180);
      }
    };

    tick();
    const timer = window.setInterval(tick, 1000);
    return () => window.clearInterval(timer);
  }, [clearUploadSessionState, paymentUploadSession, t.paymentSessionExpired]);

  useEffect(() => {
    if (!paymentUploadSession || paymentStatus !== "idle" || paymentSessionRemainingSeconds <= 0) {
      return;
    }
    if (currentStep !== 5) {
      setCurrentStep(5);
    }
  }, [currentStep, paymentSessionRemainingSeconds, paymentStatus, paymentUploadSession]);

  const refreshPaymentBanStatus = useCallback(async () => {
    if (telegramUserId <= 0) {
      setPaymentBanStatus(null);
      setPaymentBanSecondsLeft(0);
      setPaymentBanInfoLoaded(true);
      return;
    }

    try {
      const response = await fetchPaymentStatus(
        telegramUserId,
        paymentUploadSession?.sessionId ?? "",
      );
      const status = response.status;
      setPaymentBanStatus(status);
      setPaymentBanSecondsLeft(Math.max(Math.floor(status.seconds_remaining ?? 0), 0));
    } catch {
      // ignore network errors; user can retry flow manually
    } finally {
      setPaymentBanInfoLoaded(true);
    }
  }, [paymentUploadSession?.sessionId, telegramUserId]);

  useEffect(() => {
    void refreshPaymentBanStatus();
    const timer = window.setInterval(() => {
      void refreshPaymentBanStatus();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [refreshPaymentBanStatus]);

  useEffect(() => {
    if (!paymentBanStatus?.banned) {
      setPaymentBanSecondsLeft(0);
      return;
    }
    setPaymentBanSecondsLeft(Math.max(Math.floor(paymentBanStatus.seconds_remaining ?? 0), 0));
    const timer = window.setInterval(() => {
      setPaymentBanSecondsLeft((currentValue) => Math.max(currentValue - 1, 0));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [paymentBanStatus]);

  const purchasablePrivileges = useMemo(
    () => privileges.filter(isPurchasablePrivilege),
    [],
  );

  const loadServers = useCallback(async (showLoader: boolean) => {
    if (showLoader) {
      setIsLoadingServers(true);
    }

    try {
      const liveServers = await fetchServers();
      setServers(liveServers);
      setServersError(null);
    } catch {
      setServersError("Failed to load live server data");
    } finally {
      if (showLoader) {
        setIsLoadingServers(false);
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

  const loadUserBalance = useCallback(async (showLoader: boolean) => {
    if (telegramUserId <= 0) {
      setUserBalance(0);
      return;
    }
    if (showLoader) {
      setIsLoadingBalance(true);
    }
    try {
      const response = await fetchUserBalance(telegramUserId);
      setUserBalance(Math.max(0, Number(response.balance || 0)));
      setBalanceBannerMessage(null);
    } catch {
      setBalanceBannerMessage(t.balanceLoadFailed);
    } finally {
      if (showLoader) {
        setIsLoadingBalance(false);
      }
    }
  }, [t.balanceLoadFailed, telegramUserId]);

  useEffect(() => {
    void loadUserBalance(true);
    const timer = window.setInterval(() => {
      void loadUserBalance(false);
    }, 20000);
    return () => window.clearInterval(timer);
  }, [loadUserBalance]);

  useEffect(() => {
    const onTopUpSuccess = () => {
      setBalanceBannerMessage(t.balanceUpdated);
      setSubmissionError(null);
      void loadUserBalance(false);
    };
    window.addEventListener("strike:balance-topup-success", onTopUpSuccess);
    return () => {
      window.removeEventListener("strike:balance-topup-success", onTopUpSuccess);
    };
  }, [loadUserBalance, t.balanceUpdated]);

  useEffect(() => {
    if (!cashbackToast) {
      return;
    }
    const timerId = window.setTimeout(() => {
      setCashbackToast(null);
    }, 4500);
    return () => window.clearTimeout(timerId);
  }, [cashbackToast]);

  const visibleServers = useMemo(() => {
    if (!requestedPrivilege) {
      return servers;
    }

    return servers.filter((server) => serverSupportsPrivilege(server, requestedPrivilege));
  }, [servers, requestedPrivilege]);

  const publicServers = useMemo(
    () => visibleServers.filter(isPublicServer),
    [visibleServers],
  );

  const mixServers = useMemo(
    () => visibleServers.filter((server) => !isPublicServer(server)),
    [visibleServers],
  );

  const selectedServerData = useMemo(
    () => visibleServers.find((server) => server.id === selectedServer) ?? null,
    [visibleServers, selectedServer],
  );

  const availablePrivileges = useMemo(() => {
    if (!selectedServerData) {
      return [] as PurchasablePrivilege[];
    }

    const allowedPrivileges = new Set<PurchasablePrivilegeId>(
      getAllowedPrivilegeIdsForServer(selectedServerData),
    );

    return purchasablePrivileges.filter((privilege) => allowedPrivileges.has(privilege.id));
  }, [selectedServerData, purchasablePrivileges]);

  const selectedPrivilegeData = useMemo(
    () => privileges.find((privilege) => privilege.id === selectedPrivilege) ?? null,
    [selectedPrivilege],
  );

  const selectedTariff = useMemo(() => {
    if (
      !selectedServerData ||
      !selectedTariffMonths ||
      !isPurchasablePrivilegeId(selectedPrivilege)
    ) {
      return null;
    }

    return (
      getPrivilegeTariffsForServer(selectedServerData, selectedPrivilege).find(
        (tariff) => tariff.months === selectedTariffMonths,
      ) ?? null
    );
  }, [selectedServerData, selectedPrivilege, selectedTariffMonths]);

  const selectedBonusTariff = useMemo(
    () => BONUS_TARIFFS.find((tariff) => tariff.id === selectedBonusTariffId) ?? null,
    [selectedBonusTariffId],
  );

  const selectedProductType: ProductType | null = useMemo(() => {
    if (selectedBonusTariff) {
      return "bonus";
    }
    if (selectedTariff) {
      return "privilege";
    }
    return null;
  }, [selectedBonusTariff, selectedTariff]);

  const isBonusFlow = selectedProductType === "bonus";
  const isSelectedPrivilegeNicknameOnly = useMemo(
    () =>
      selectedProductType === "privilege" &&
      NICKNAME_ONLY_PRIVILEGE_IDS.has(selectedPrivilege.trim().toLowerCase()),
    [selectedPrivilege, selectedProductType],
  );
  const isPrivilegeSteamMode = selectedProductType === "privilege" && privilegeIdentifierType === "steam";
  const isPrivilegeNicknameMode =
    selectedProductType === "privilege" && privilegeIdentifierType === "nickname";
  const isPrivilegePermanentBlocked = Boolean(
    selectedProductType === "privilege" &&
    privilegeAccountInfo?.exists &&
    privilegeAccountInfo?.isPermanent,
  );
  const isPrivilegeRenewalFlow = Boolean(
    selectedProductType === "privilege" &&
    privilegeAccountInfo?.exists &&
    !privilegeAccountInfo?.isPermanent,
  );
  const privilegePaymentAdjustment = useMemo<PrivilegePaymentAdjustment>(() => {
    const defaultAmount = selectedTariff?.finalPrice ?? 0;
    const targetPrivilegeId = normalizePublicStyle1PrivilegeId(selectedPrivilege);
    const existingPrivilegeId = resolveExistingPublicStyle1PrivilegeId(privilegeAccountInfo);
    const daysLeft = Math.max(privilegeAccountInfo?.days ?? 0, 0);
    const isExistingActive = Boolean(
      privilegeAccountInfo?.exists &&
      !privilegeAccountInfo?.isDisabled &&
      daysLeft > 0 &&
      existingPrivilegeId,
    );

    if (!selectedServerData || !selectedTariff || !isPublicStyle1Server(selectedServerData) || !targetPrivilegeId) {
      return {
        finalAmount: defaultAmount,
        creditAmount: 0,
        isUpgradeWithCredit: false,
        isDowngradeBlocked: false,
        existingPrivilegeId,
        targetPrivilegeId,
        isExistingActive,
      };
    }

    if (!isExistingActive || !existingPrivilegeId) {
      return {
        finalAmount: defaultAmount,
        creditAmount: 0,
        isUpgradeWithCredit: false,
        isDowngradeBlocked: false,
        existingPrivilegeId,
        targetPrivilegeId,
        isExistingActive,
      };
    }

    const existingTier = PUBLIC_STYLE_1_PRIVILEGE_TIER[existingPrivilegeId];
    const targetTier = PUBLIC_STYLE_1_PRIVILEGE_TIER[targetPrivilegeId];

    if (targetTier < existingTier) {
      return {
        finalAmount: defaultAmount,
        creditAmount: 0,
        isUpgradeWithCredit: false,
        isDowngradeBlocked: true,
        existingPrivilegeId,
        targetPrivilegeId,
        isExistingActive,
      };
    }

    if (targetTier > existingTier) {
      const creditRaw = (PUBLIC_STYLE_1_ONE_MONTH_PRICE_BY_ID[existingPrivilegeId] / 30) * daysLeft;
      const creditAmount = Math.max(0, Math.floor(creditRaw));
      const recalculatedAmount = floorToNearest500(defaultAmount - creditRaw);

      return {
        finalAmount: recalculatedAmount,
        creditAmount,
        isUpgradeWithCredit: true,
        isDowngradeBlocked: false,
        existingPrivilegeId,
        targetPrivilegeId,
        isExistingActive,
      };
    }

    return {
      finalAmount: defaultAmount,
      creditAmount: 0,
      isUpgradeWithCredit: false,
      isDowngradeBlocked: false,
      existingPrivilegeId,
      targetPrivilegeId,
      isExistingActive,
    };
  }, [privilegeAccountInfo, selectedPrivilege, selectedServerData, selectedTariff]);
  const isPrivilegeDowngradeBlocked = Boolean(
    isPrivilegeRenewalFlow && privilegePaymentAdjustment.isDowngradeBlocked,
  );
  const effectivePrivilegePassword = useMemo(() => {
    if (isPrivilegeSteamMode) {
      return "";
    }
    if (isPrivilegeRenewalFlow) {
      if (changePasswordChoice) {
        return newPassword.trim();
      }
      return currentPassword.trim();
    }
    return password.trim();
  }, [
    changePasswordChoice,
    currentPassword,
    isPrivilegeSteamMode,
    isPrivilegeRenewalFlow,
    newPassword,
    password,
  ]);
  const selectedPrice = selectedBonusTariff?.price ?? privilegePaymentAdjustment.finalAmount ?? 0;
  const selectedPrivilegeCashbackPercent =
    selectedProductType === "privilege" && selectedPrivilegeData
      ? getPrivilegeCashbackPercent(selectedPrivilegeData.id)
      : 0;
  const selectedPrivilegeCashbackAmount =
    selectedProductType === "privilege" && selectedPrivilegeCashbackPercent > 0
      ? calculateCashbackAmount(selectedPrice, selectedPrivilegeCashbackPercent)
      : 0;
  const hasEnoughBalance = selectedPrice <= 0 || userBalance >= selectedPrice;
  const missingBalanceAmount = Math.max(selectedPrice - userBalance, 0);
  const isPaymentBanActive = Boolean(paymentBanStatus?.banned && paymentBanSecondsLeft > 0);
  const paymentBanCountdownLabel = formatCountdown(paymentBanSecondsLeft);
  const paymentBanMessage = isPaymentBanActive
    ? (
      isUz
        ? `${paymentBanStatus?.reason || t.paymentBannedHint} Qayta urinish ${paymentBanCountdownLabel} dan keyin.`
        : `${paymentBanStatus?.reason || t.paymentBannedHint} Повторите попытку через ${paymentBanCountdownLabel}.`
    )
    : "";
  const isPaymentSessionActive = Boolean(
    paymentUploadSession &&
    paymentStatus === "idle" &&
    paymentSessionRemainingSeconds > 0,
  );
  const passwordCommand = useMemo(
    () => `setinfo _pw ${effectivePrivilegePassword}`,
    [effectivePrivilegePassword],
  );

  useEffect(() => {
    if (selectedProductType !== "privilege") {
      return;
    }
    if (!isSelectedPrivilegeNicknameOnly) {
      return;
    }
    if (privilegeIdentifierType !== "nickname") {
      setPrivilegeIdentifierType("nickname");
      setSteamId("");
      setSubmissionError(null);
    }
  }, [
    isSelectedPrivilegeNicknameOnly,
    privilegeIdentifierType,
    selectedProductType,
  ]);

  useEffect(() => {
    if (!selectedServer) {
      return;
    }

    const stillVisible = visibleServers.some((server) => server.id === selectedServer);
    if (!stillVisible) {
      if (isPaymentSessionActive || currentStep >= 5) {
        return;
      }
      setSelectedServer("");
      setSelectedPrivilege("");
      setSelectedTariffMonths(null);
      setSelectedBonusTariffId(null);
      setIsBonusExpanded(false);
      setSteamId("");
      setBonusAccountInfo(null);
      setPrivilegeAccountInfo(null);
      setPrivilegeIdentifierType("nickname");
      setRenewalRequested(false);
      setCurrentPassword("");
      setNewPassword("");
      setChangePasswordChoice(null);
      setCurrentPasswordVerified(false);
      setExpandedPrivilegeId(requestedPrivilege);
      setCurrentStep(1);
      setPaymentStatus("idle");
      setSubmissionError(null);
      setPasswordCopied(false);
      setCardCopied(false);
    }
  }, [currentStep, isPaymentSessionActive, selectedServer, visibleServers, requestedPrivilege]);

  useEffect(() => {
    if (!selectedServerData || !isPurchasablePrivilegeId(selectedPrivilege)) {
      return;
    }

    const tariffs = getPrivilegeTariffsForServer(selectedServerData, selectedPrivilege);
    if (!tariffs.some((tariff) => tariff.months === selectedTariffMonths)) {
      setSelectedTariffMonths(tariffs[0]?.months ?? null);
    }
  }, [selectedServerData, selectedPrivilege, selectedTariffMonths]);

  useEffect(() => {
    setBonusAccountInfo(null);
  }, [normalizedSteamId, selectedServer]);

  useEffect(() => {
    setPrivilegeAccountInfo(null);
    setRenewalRequested(false);
    setCurrentPassword("");
    setNewPassword("");
    setChangePasswordChoice(null);
    setCurrentPasswordVerified(false);
  }, [nickname, normalizedSteamId, privilegeIdentifierType, selectedServer, selectedPrivilege, selectedTariffMonths]);

  const resetPrivilegeRenewalState = useCallback(() => {
    setPrivilegeAccountInfo(null);
    setRenewalRequested(false);
    setCurrentPassword("");
    setNewPassword("");
    setChangePasswordChoice(null);
    setCurrentPasswordVerified(false);
    setIsResolvingPrivilegeAccount(false);
    setIsVerifyingCurrentPassword(false);
  }, []);

  useEffect(() => {
    if (!requestedRenewFlow || renewPrefillAppliedRef.current) {
      return;
    }
    if (!requestedServerId || !requestedPrivilege || servers.length === 0) {
      return;
    }

    const targetServer = servers.find((item) => item.id === requestedServerId);
    if (!targetServer || !serverSupportsPrivilege(targetServer, requestedPrivilege)) {
      return;
    }

    setSelectedServer(targetServer.id);
    setSelectedPrivilege(requestedPrivilege);
    setExpandedPrivilegeId(requestedPrivilege);
    setSelectedTariffMonths(null);
    setSelectedBonusTariffId(null);
    setIsBonusExpanded(false);
    setCurrentStep(2);

    if (requestedIdentifierType === "steam") {
      setPrivilegeIdentifierType("steam");
      setSteamId(requestedSteamId);
      setNickname("");
    } else {
      setPrivilegeIdentifierType("nickname");
      setNickname(requestedNickname);
      setSteamId("");
    }

    setSubmissionError(null);
    setPurchaseResponse(null);
    setPaymentStatus("idle");
    setPasswordCopied(false);
    setCardCopied(false);
    purchaseNotificationSentRef.current = false;
    resetPrivilegeRenewalState();
    setPassword(requestedPassword);
    setCurrentPassword(requestedPassword);
    setCurrentPasswordVerified(false);
    renewPrefillAppliedRef.current = true;
  }, [
    requestedRenewFlow,
    requestedServerId,
    requestedPrivilege,
    servers,
    requestedIdentifierType,
    requestedNickname,
    requestedPassword,
    requestedSteamId,
    resetPrivilegeRenewalState,
  ]);

  const handleServerSelect = (server: LiveServer) => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    setSelectedServer(server.id);
    setPaymentStatus("idle");
    setSubmissionError(null);
    setPurchaseResponse(null);
    setPasswordCopied(false);
    setCardCopied(false);
    purchaseNotificationSentRef.current = false;
    setPrivilegeIdentifierType("nickname");
    setNickname("");
    setPassword("");
    setSteamId("");
    setBonusAccountInfo(null);
    resetPrivilegeRenewalState();
    setScreenshot(null);
    setScreenshotPreviewUrl((currentUrl) => {
      if (currentUrl) {
        URL.revokeObjectURL(currentUrl);
      }
      return null;
    });
    clearUploadSessionState();

    if (requestedPrivilege && serverSupportsPrivilege(server, requestedPrivilege)) {
      const defaultTariff =
        getPrivilegeTariffsForServer(server, requestedPrivilege).find(
          (tariff) => tariff.months === 1,
        ) ??
        getPrivilegeTariffsForServer(server, requestedPrivilege)[0] ??
        null;

      setSelectedPrivilege(requestedPrivilege);
      setExpandedPrivilegeId(requestedPrivilege);
      setSelectedTariffMonths(defaultTariff ? defaultTariff.months : null);
      setSelectedBonusTariffId(null);
      setIsBonusExpanded(false);
      setCurrentStep(defaultTariff ? 3 : 2);
      return;
    }

    setSelectedPrivilege("");
    setSelectedTariffMonths(null);
    setSelectedBonusTariffId(null);
    setIsBonusExpanded(false);
    setExpandedPrivilegeId(null);
    setCurrentStep(2);
  };

  const handlePrivilegeToggle = (privilegeId: PurchasablePrivilegeId) => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    setExpandedPrivilegeId((currentValue) =>
      currentValue === privilegeId ? null : privilegeId,
    );
  };

  const handleBonusToggle = () => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    setIsBonusExpanded((currentValue) => !currentValue);
  };

  const handlePrivilegeIdentifierTypeChange = (nextType: PrivilegeIdentifierType) => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    if (nextType === privilegeIdentifierType) {
      return;
    }
    if (nextType === "steam" && isSelectedPrivilegeNicknameOnly) {
      return;
    }
    setPrivilegeIdentifierType(nextType);
    setSubmissionError(null);
    setNickname("");
    setPassword("");
    setCurrentPassword("");
    setNewPassword("");
    setChangePasswordChoice(null);
    setCurrentPasswordVerified(false);
    setPrivilegeAccountInfo(null);
    setRenewalRequested(false);
    setSteamId("");
  };

  const handleTariffSelect = (
    privilegeId: PurchasablePrivilegeId,
    months: TariffOption["months"],
  ) => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    setSelectedPrivilege(privilegeId);
    setSelectedTariffMonths(months);
    setSelectedBonusTariffId(null);
    setIsBonusExpanded(false);
    setExpandedPrivilegeId(privilegeId);
    setPaymentStatus("idle");
    setSubmissionError(null);
    setPurchaseResponse(null);
    setPrivilegeIdentifierType("nickname");
    setNickname("");
    setPassword("");
    setSteamId("");
    setBonusAccountInfo(null);
    resetPrivilegeRenewalState();
    setCardCopied(false);
    purchaseNotificationSentRef.current = false;
    clearUploadSessionState();
    scrollActionsToScreenMiddle(120);
  };

  const handleBonusTariffSelect = (tariffId: BonusTariffId) => {
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }
    if (isPaymentSessionActive) {
      return;
    }
    setSelectedBonusTariffId(tariffId);
    setIsBonusExpanded(true);
    setSelectedPrivilege("");
    setSelectedTariffMonths(null);
    setExpandedPrivilegeId(null);
    setPaymentStatus("idle");
    setSubmissionError(null);
    setPurchaseResponse(null);
    setSteamId("");
    setBonusAccountInfo(null);
    resetPrivilegeRenewalState();
    setCardCopied(false);
    purchaseNotificationSentRef.current = false;
    clearUploadSessionState();
    scrollActionsToScreenMiddle(120);
  };

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    if (!isPaymentSessionActive || isPaymentBanActive) {
      return;
    }
    if (!event.target.files || !event.target.files[0]) {
      return;
    }

    const file = event.target.files[0];
    const nextPreviewUrl = URL.createObjectURL(file);

    setScreenshot(file);
    setScreenshotPreviewUrl((currentUrl) => {
      if (currentUrl) {
        URL.revokeObjectURL(currentUrl);
      }
      return nextPreviewUrl;
    });
  };

  const handleCopyPasswordCommand = () => {
    if (!effectivePrivilegePassword) {
      return;
    }

    navigator.clipboard.writeText(passwordCommand);
    setPasswordCopied(true);
    window.setTimeout(() => setPasswordCopied(false), 2000);
  };

  const handleCopyCardNumber = () => {
    navigator.clipboard.writeText(paymentCardNumberRaw.replace(/\s+/g, ""));
    setCardCopied(true);
    window.setTimeout(() => setCardCopied(false), 2000);
  };

  const handleOpenPaymentApp = (app: PaymentApp) => {
    const telegramWebApp = (
      window as Window & {
        Telegram?: {
          WebApp?: {
            openLink?: (url: string) => void;
          };
        };
      }
    ).Telegram?.WebApp;

    if (isIOSDevice) {
      // iOS Telegram WebView is strict; try several direct deeplink launch methods.
      try {
        if (typeof telegramWebApp?.openLink === "function") {
          telegramWebApp.openLink(app.deepLink);
          return;
        }
      } catch {
        // ignore and try next method
      }

      try {
        window.location.href = app.deepLink;
        return;
      } catch {
        // ignore and try anchor fallback
      }

      const anchor = document.createElement("a");
      anchor.href = app.deepLink;
      anchor.target = "_self";
      anchor.rel = "noopener noreferrer";
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      return;
    }

    const launchUrl = isAndroidDevice && app.androidIntent ? app.androidIntent : app.deepLink;
    window.location.assign(launchUrl);
  };

  const resolveBonusAccount = useCallback(async (): Promise<boolean> => {
    if (!selectedServerData || !selectedBonusTariff || !serverSupportsBonus(selectedServerData)) {
      return false;
    }

    if (!steamIdValidation.isValid) {
      return false;
    }

    setIsResolvingStep3(true);
    setSubmissionError(null);
    try {
      const account = await fetchBonusAccount(selectedServerData.id, normalizedSteamId);
      setBonusAccountInfo(account);
      return true;
    } catch {
      setBonusAccountInfo(null);
      setSubmissionError(t.steamLookupFailed);
      return false;
    } finally {
      setIsResolvingStep3(false);
    }
  }, [
    normalizedSteamId,
    selectedBonusTariff,
    selectedServerData,
    steamIdValidation.isValid,
    t.steamLookupFailed,
  ]);

  const resolvePrivilegeAccount = useCallback(async (): Promise<boolean> => {
    if (!selectedServerData || !selectedTariff || !selectedPrivilegeData) {
      return false;
    }

    if (isPrivilegeSteamMode) {
      if (!steamIdValidation.isValid) {
        return false;
      }
    } else if (!nicknameValidation.isValid) {
      return false;
    }

    setIsResolvingPrivilegeAccount(true);
    setSubmissionError(null);
    try {
      const account = await fetchPrivilegeAccount({
        serverId: selectedServerData.id,
        serverName: selectedServerData.name,
        identifierType: isPrivilegeSteamMode ? "steam" : "nickname",
        nickname: isPrivilegeSteamMode ? "" : nickname.trim(),
        steamId: isPrivilegeSteamMode ? normalizedSteamId : "",
      });
      if (account.exists) {
        setPrivilegeAccountInfo(account);
        setPassword("");
        const prefilledCurrentPassword = String(account.password ?? "").trim();
        const canUsePrefilledCurrentPassword =
          /^[A-Za-z0-9]{1,20}$/.test(prefilledCurrentPassword);
        if (!isPrivilegeSteamMode && canUsePrefilledCurrentPassword) {
          setCurrentPassword(prefilledCurrentPassword);
          setCurrentPasswordVerified(true);
          setChangePasswordChoice(false);
        } else {
          setCurrentPassword("");
          setCurrentPasswordVerified(false);
          setChangePasswordChoice(null);
        }
      } else {
        setPrivilegeAccountInfo(null);
        setRenewalRequested(false);
        setCurrentPassword("");
        setCurrentPasswordVerified(false);
        setChangePasswordChoice(null);
      }
      setNewPassword("");
      return true;
    } catch (error) {
      const errorMessage = extractReadableErrorMessage(error);
      setSubmissionError(errorMessage || t.privilegeLookupFailed);
      return false;
    } finally {
      setIsResolvingPrivilegeAccount(false);
    }
  }, [
    isPrivilegeSteamMode,
    nicknameValidation.isValid,
    nickname,
    normalizedSteamId,
    selectedPrivilegeData,
    selectedServerData,
    selectedTariff,
    steamIdValidation.isValid,
    t.privilegeLookupFailed,
  ]);

  const verifyCurrentPasswordForRenewal = useCallback(async (): Promise<boolean> => {
    if (isPrivilegeSteamMode) {
      setCurrentPasswordVerified(true);
      return true;
    }

    if (!selectedServerData || !privilegeAccountInfo?.exists) {
      return false;
    }

    if (!currentPasswordValidation.isValid) {
      return false;
    }

    setIsVerifyingCurrentPassword(true);
    setSubmissionError(null);
    try {
      const response = await verifyPrivilegePassword(
        selectedServerData.id,
        nickname.trim(),
        currentPassword.trim(),
        selectedServerData.name,
      );
      if (!response.valid) {
        setCurrentPasswordVerified(false);
        setSubmissionError(t.currentPasswordInvalid);
        return false;
      }
      setCurrentPasswordVerified(true);
      return true;
    } catch {
      setCurrentPasswordVerified(false);
      setSubmissionError(t.currentPasswordInvalid);
      return false;
    } finally {
      setIsVerifyingCurrentPassword(false);
    }
  }, [
    currentPassword,
    currentPasswordValidation.isValid,
    isPrivilegeSteamMode,
    nickname,
    privilegeAccountInfo?.exists,
    selectedServerData,
    t.currentPasswordInvalid,
  ]);

  const handleSubmit = async () => {
    if (!selectedServerData || !selectedProductType) {
      setSubmissionError(t.submitFailed);
      return;
    }
    if (!hasEnoughBalance) {
      setSubmissionError(
        `${t.insufficientBalanceHint} ${t.missingAmountLabel}: ${formatBalanceMoney(missingBalanceAmount)} UZS.`,
      );
      return;
    }

    setPaymentStatus("processing");
    setSubmissionError(null);
    setPasswordCopied(false);
    purchaseNotificationSentRef.current = false;

    const telegram = (
      window as Window & {
        Telegram?: {
          WebApp?: {
            sendData?: (data: string) => void;
            initDataUnsafe?: {
              user?: {
                id?: number;
                username?: string;
                first_name?: string;
                last_name?: string;
              };
            };
          };
        };
      }
    ).Telegram;
    const webApp = telegram?.WebApp;
    const telegramUser = webApp?.initDataUnsafe?.user;
    const currentTelegramUserId = Number(telegramUser?.id ?? 0);

    if (currentTelegramUserId <= 0) {
      setPaymentStatus("idle");
      setSubmissionError(t.submitFailed);
      return;
    }

    try {
      let response: PurchaseConfirmedResponse;

      if (selectedProductType === "bonus") {
        if (!selectedBonusTariff || !bonusAccountInfo || !steamIdValidation.isValid) {
          throw new Error("Bonus purchase state is invalid");
        }

        response = await notifyPurchaseConfirmed({
          userId: currentTelegramUserId,
          productType: "bonus",
          serverId: selectedServerData.id,
          server: selectedServerData.name,
          amount: selectedBonusTariff.price,
          steamId: normalizedSteamId,
          bonusAmount: selectedBonusTariff.bonusAmount,
          bonusPackageLabel: `${formatPrice(selectedBonusTariff.bonusAmount)} ${t.bonusPackage}`,
          bonusNickname: bonusAccountInfo.nickname,
          bonusBefore: bonusAccountInfo.bonusCount,
          username: telegramUser?.username ?? "",
          firstName: telegramUser?.first_name ?? "",
          lastName: telegramUser?.last_name ?? "",
          useBalance: true,
          language,
        });
      } else {
        if (!selectedPrivilegeData || !selectedTariff) {
          throw new Error("Privilege purchase state is invalid");
        }

        const isRenewal = Boolean(privilegeAccountInfo?.exists);
        const isSteamIdentifier = privilegeIdentifierType === "steam";
        const finalPassword = isSteamIdentifier
          ? ""
          : (isRenewal ? effectivePrivilegePassword : password.trim());
        if (
          !isSteamIdentifier &&
          (!finalPassword || validatePassword(finalPassword, language).isValid === false)
        ) {
          throw new Error("Privilege password state is invalid");
        }

        response = await notifyPurchaseConfirmed({
          userId: currentTelegramUserId,
          productType: "privilege",
          identifierType: privilegeIdentifierType,
          serverId: selectedServerData.id,
          privilege: selectedPrivilegeData.name,
          server: selectedServerData.name,
          duration: tariffLabel(selectedTariff.months, language),
          durationMonths: selectedTariff.months,
          amount: selectedPrice,
          nickname: isSteamIdentifier ? "" : nickname.trim(),
          steamId: isSteamIdentifier ? normalizedSteamId : undefined,
          password: finalPassword,
          renewalRequested: isRenewal,
          currentPassword: isRenewal && !isSteamIdentifier ? currentPassword.trim() : undefined,
          changePassword:
            isRenewal && !isSteamIdentifier ? Boolean(changePasswordChoice) : undefined,
          username: telegramUser?.username ?? "",
          firstName: telegramUser?.first_name ?? "",
          lastName: telegramUser?.last_name ?? "",
          useBalance: true,
          language,
        });
      }

      purchaseNotificationSentRef.current = true;
      setPurchaseResponse(response);
      if (response.balance) {
        setUserBalance(Math.max(0, Number(response.balance.balance || 0)));
      } else {
        void loadUserBalance(false);
      }
      setPaymentStatus("success");
      const cashbackAmount = Math.max(0, Number(response.cashback?.amount || 0));
      const cashbackPercent = Math.max(0, Number(response.cashback?.percent || 0));
      if (cashbackAmount > 0) {
        const cashbackMessage = `${t.cashbackCredited}: +${formatBalanceMoney(cashbackAmount)} UZS (${cashbackPercent}%)`;
        setBalanceBannerMessage(cashbackMessage);
        setCashbackToast({ amount: cashbackAmount, percent: cashbackPercent });
        triggerSuccessHaptic();
      } else {
        setBalanceBannerMessage(t.balanceUpdated);
      }
    } catch (error) {
      setPaymentStatus("idle");
      const errorMessage = extractReadableErrorMessage(error);
      setSubmissionError(errorMessage || t.submitFailed);
      void loadUserBalance(false);
    }
  };

  const handleNextStep = useCallback(async () => {
    setSubmissionError(null);
    if (isPaymentBanActive) {
      setSubmissionError(paymentBanMessage);
      return;
    }

    if (currentStep === 2) {
      if (selectedProductType) {
        setCurrentStep(3);
      }
      return;
    }

    if (currentStep === 3) {
      if (isBonusFlow) {
        const resolved = await resolveBonusAccount();
        if (resolved) {
          setCurrentStep(4);
        }
        return;
      }

      const canResolvePrivilegeAccount = isPrivilegeSteamMode
        ? steamIdValidation.isValid
        : nicknameValidation.isValid;
      if (canResolvePrivilegeAccount) {
        const resolved = await resolvePrivilegeAccount();
        if (resolved) {
          setCurrentStep(4);
        }
      }
      return;
    }

    if (currentStep === 4) {
      if (isBonusFlow) {
        if (bonusAccountInfo && steamIdValidation.isValid) {
          setCurrentStep(5);
        }
        return;
      }

      if (isPrivilegePermanentBlocked) {
        setSubmissionError(t.permanentPrivilegeBlocked);
        return;
      }

      if (isPrivilegeRenewalFlow) {
        if (isPrivilegeDowngradeBlocked) {
          setSubmissionError(t.downgradeBlocked);
          return;
        }

        if (!renewalRequested) {
          setSubmissionError(t.renewSelectRequired);
          return;
        }

        if (isPrivilegeSteamMode) {
          setCurrentStep(5);
          return;
        }

        if (!currentPasswordVerified) {
          const verified = await verifyCurrentPasswordForRenewal();
          if (!verified) {
            return;
          }
          return;
        }

        if (changePasswordChoice === null) {
          return;
        }

        if (changePasswordChoice && !newPasswordValidation.isValid) {
          return;
        }

        setCurrentStep(5);
        return;
      }

      if (isPrivilegeSteamMode) {
        setCurrentStep(5);
        return;
      }

      if (passwordValidation.isValid) {
        setCurrentStep(5);
      }
    }
  }, [
    bonusAccountInfo,
    changePasswordChoice,
    currentPasswordVerified,
    currentStep,
    isPaymentBanActive,
    isBonusFlow,
    isPrivilegeDowngradeBlocked,
    isPrivilegePermanentBlocked,
    isPrivilegeRenewalFlow,
    isPrivilegeSteamMode,
    nicknameValidation.isValid,
    newPasswordValidation.isValid,
    passwordValidation.isValid,
    paymentBanMessage,
    renewalRequested,
    resolveBonusAccount,
    resolvePrivilegeAccount,
    selectedProductType,
    steamIdValidation.isValid,
    t.downgradeBlocked,
    t.permanentPrivilegeBlocked,
    t.renewSelectRequired,
    verifyCurrentPasswordForRenewal,
  ]);

  const handleBackStep = useCallback(() => {
    if (
      isResolvingStep3 ||
      isResolvingPrivilegeAccount ||
      currentStep <= 1 ||
      (currentStep === 5 && isPaymentSessionActive)
    ) {
      return;
    }
    setSubmissionError(null);
    setCurrentStep((currentStep - 1) as Step);
  }, [currentStep, isPaymentSessionActive, isResolvingPrivilegeAccount, isResolvingStep3]);

  const handleNicknameFocus = useCallback(() => {
    scrollElementToTop(nicknameStepCardRef.current, 60);
  }, [scrollElementToTop]);

  const handlePasswordFocus = useCallback(() => {
    scrollElementToTop(passwordStepCardRef.current, 60);
  }, [scrollElementToTop]);

  useEffect(() => {
    if (keyboardInset <= 0) {
      return;
    }

    if (currentStep === 3) {
      scrollElementToTop(nicknameStepCardRef.current, 80);
      return;
    }

    if (currentStep === 4) {
      scrollElementToTop(passwordStepCardRef.current, 80);
    }
  }, [
    currentStep,
    keyboardInset,
    scrollElementToTop,
  ]);

  const pageBottomPadding = useMemo(() => {
    const basePadding = 96;
    if (keyboardInset <= 0) {
      return basePadding;
    }
    return basePadding + keyboardInset + 24;
  }, [keyboardInset]);

  const canProceedStep2 = selectedProductType !== null && !isPaymentBanActive;
  const canProceedStep3 = isBonusFlow
    ? steamIdValidation.isValid && !isResolvingStep3 && !isPaymentBanActive
    : (
        isPrivilegeSteamMode
          ? steamIdValidation.isValid && !isResolvingPrivilegeAccount && !isPaymentBanActive
          : nicknameValidation.isValid && !isResolvingPrivilegeAccount && !isPaymentBanActive
      );
  const canProceedStep4 = isBonusFlow
    ? bonusAccountInfo !== null && steamIdValidation.isValid && !isPaymentBanActive
    : (
        isPrivilegePermanentBlocked
          ? false
          : isPrivilegeRenewalFlow
          ? (
              !isPrivilegeDowngradeBlocked &&
              renewalRequested &&
              (
                isPrivilegeSteamMode
                  ? true
                  : (
                      !isVerifyingCurrentPassword &&
                      (
                        !currentPasswordVerified
                          ? currentPasswordValidation.isValid
                          : (
                              changePasswordChoice !== null &&
                              (changePasswordChoice ? newPasswordValidation.isValid : true)
                            )
                      )
                    )
              )
            )
          : (isPrivilegeSteamMode ? steamIdValidation.isValid : passwordValidation.isValid)
      ) && !isPaymentBanActive;
  const canProceedStep5 =
    selectedServerData !== null &&
    selectedProductType !== null &&
    hasEnoughBalance &&
    !isPaymentBanActive;
  const showBonusOption = Boolean(selectedServerData && serverSupportsBonus(selectedServerData));

  const renderServerSkeleton = (index: number) => (
    <div
      key={`server-skeleton-${index}`}
      className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 animate-pulse"
    >
      <div className="h-4 bg-[#2a2a2a] rounded w-4/5" />
    </div>
  );

  const renderServerButton = (server: LiveServer) => (
    <button
      key={server.id}
      onClick={() => handleServerSelect(server)}
      className={`w-full text-left p-3 rounded-lg border transition-all active:scale-95 ${
        selectedServer === server.id
          ? "bg-[#F08800]/10 border-[#F08800] shadow-lg shadow-[#F08800]/10"
          : "bg-[#121212] border-[#2a2a2a] hover:border-[#F08800]/50"
      }`}
    >
      <div className="text-[#FCFCFC] font-bold text-sm">{server.name}</div>
    </button>
  );

  const renderServerSection = (
    title: string,
    description: string,
    sectionServers: LiveServer[],
  ) => (
    <section className="space-y-2.5" key={title}>
      <div className="px-1 pt-1">
        <h3 className="text-[#FCFCFC] text-lg font-black uppercase tracking-wide">{title}</h3>
        <p className="text-[#888888] text-xs mt-1">{description}</p>
      </div>
      <div className="space-y-2.5">{sectionServers.map(renderServerButton)}</div>
    </section>
  );

  return (
    <PageTransition>
      <div
        className="px-3 py-4"
        style={{ paddingBottom: `${pageBottomPadding}px` }}
      >
        <div className="space-y-4">
          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-3.5">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <p className="text-[#888888] text-xs uppercase tracking-[0.12em]">{t.balanceTitle}</p>
                <p className="text-[#FCFCFC] text-xl font-black mt-1">
                  {isLoadingBalance ? "..." : `${formatBalanceMoney(userBalance)} UZS`}
                </p>
              </div>
              <button
                type="button"
                onClick={openTopUp}
                className="flex items-center gap-2 bg-[#121212] border border-[#F08800]/40 rounded-lg px-3 py-2.5 text-[#FCFCFC] hover:border-[#F08800]/70 transition-all"
              >
                <WalletCards className="w-4 h-4 text-[#F08800]" strokeWidth={2.2} />
                <span className="text-xs font-bold uppercase tracking-wide">{t.topUpBalance}</span>
              </button>
            </div>
            {balanceBannerMessage && (
              <p className="text-[#86efac] text-xs mt-2.5">{balanceBannerMessage}</p>
            )}
          </div>

          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              {[1, 2, 3, 4, 5].map((step) => (
                <div key={step} className="flex items-center">
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center font-bold transition-all ${
                      step === currentStep
                        ? "bg-[#F08800] text-[#121212] shadow-lg shadow-[#F08800]/30 scale-110"
                        : step < currentStep
                          ? "bg-green-500 text-white"
                          : "bg-[#2a2a2a] text-[#888888]"
                    }`}
                  >
                    {step < currentStep ? <Check className="w-4 h-4" strokeWidth={3} /> : step}
                  </div>
                  {step < 5 && (
                    <div
                      className={`w-4 h-0.5 mx-1 ${
                        step < currentStep ? "bg-green-500" : "bg-[#2a2a2a]"
                      }`}
                    />
                  )}
                </div>
              ))}
            </div>
            <div className="text-center text-[#888888] text-xs mt-2 font-semibold">
              {t.step} {currentStep} {t.of} 5
            </div>
          </div>

          {!paymentBanInfoLoaded && !paymentUploadSession && (
            <div className="flex justify-center items-center py-10">
              <span className="text-[#555555] text-sm">···</span>
            </div>
          )}

          {paymentBanInfoLoaded && isPaymentBanActive && (
            <div className="bg-[#7f1d1d]/55 border-2 border-[#ef4444]/80 rounded-xl p-6 shadow-[0_0_32px_rgba(239,68,68,0.25)]">
              <p className="text-[#fca5a5] text-base font-black uppercase tracking-widest mb-3">
                {t.paymentBannedTitle}
              </p>
              <p className="text-[#fecaca] text-sm leading-relaxed">
                {paymentBanStatus?.reason || t.paymentBannedHint}
              </p>
              <div className="mt-4 bg-[#121212] border border-[#ef4444]/40 rounded-lg px-4 py-3 inline-flex items-center gap-3">
                <span className="text-[#fecaca] text-xs font-semibold">
                  {isUz ? "Qayta urinish:" : "Повторите через:"}
                </span>
                <span className="text-[#fca5a5] text-2xl font-black tracking-widest font-mono">
                  {paymentBanCountdownLabel}
                </span>
              </div>
            </div>
          )}

          {(paymentBanInfoLoaded || paymentUploadSession) && !isPaymentBanActive && (
          <>
          {currentStep === 1 && (
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3 mb-4">
                <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center flex-shrink-0">
                  <Server className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <h2 className="text-[#FCFCFC] text-lg">{t.serverSelection}</h2>
              </div>

              {requestedPrivilegeInfo && (
                <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                  <p className="text-[#888888] text-xs">
                    {t.availableServersForPrivilege}{" "}
                    <span className="text-[#F08800] font-bold">{requestedPrivilegeInfo.name}</span>
                  </p>
                </div>
              )}

              {serversError && visibleServers.length === 0 ? (
                <div className="bg-[#121212] border border-[#ef4444]/40 rounded-lg p-4 text-center">
                  <p className="text-[#FCFCFC] mb-3">{serversError}</p>
                  <button
                    onClick={() => void loadServers(true)}
                    className="bg-[#F08800] text-[#121212] font-bold px-4 py-2 rounded-lg"
                  >
                    {t.retry}
                  </button>
                </div>
              ) : (
                <div className="space-y-4 max-h-96 overflow-y-auto pr-1">
                  {isLoadingServers && servers.length === 0
                    ? [0, 1, 2, 3, 4].map(renderServerSkeleton)
                    : (
                        <>
                          {visibleServers.length === 0 ? (
                            <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-4 text-center text-[#888888] text-sm">
                              {t.noServersForPrivilege}
                            </div>
                          ) : (
                            <>
                              {publicServers.length > 0 &&
                                renderServerSection(
                                  t.publicServers,
                                  t.publicDesc,
                                  publicServers,
                                )}
                              {mixServers.length > 0 &&
                                renderServerSection(
                                  t.mixServers,
                                  t.mixDesc,
                                  mixServers,
                                )}
                            </>
                          )}
                        </>
                      )}
                </div>
              )}
            </div>
          )}

          {currentStep === 2 && (
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3 mb-4">
                <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center flex-shrink-0">
                  <Award className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <div>
                  <h2 className="text-[#FCFCFC] text-lg">{t.privilegeSelection}</h2>
                  {selectedServerData && (
                    <p className="text-[#888888] text-xs">{selectedServerData.name}</p>
                  )}
                </div>
              </div>

              {availablePrivileges.length === 0 && !showBonusOption ? (
                <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-4 text-center text-[#888888] text-sm">
                  {t.noPrivilegesForServer}
                </div>
              ) : (
                <div className="space-y-3">
                  {availablePrivileges.map((privilege) => {
                    const tariffs = selectedServerData
                      ? getPrivilegeTariffsForServer(selectedServerData, privilege.id)
                      : [];

                    if (tariffs.length === 0) {
                      return null;
                    }

                    const oneMonthTariff =
                      tariffs.find((tariff) => tariff.months === 1) ?? tariffs[0];
                    const isExpanded = expandedPrivilegeId === privilege.id;
                    const selectedOnThisPrivilege = selectedPrivilege === privilege.id;

                    return (
                      <div
                        key={privilege.id}
                        className={`rounded-lg border transition-all ${
                          selectedOnThisPrivilege
                            ? "bg-[#F08800]/10 border-[#F08800] shadow-lg shadow-[#F08800]/10"
                            : "bg-[#121212] border-[#2a2a2a]"
                        }`}
                      >
                        <button
                          onClick={() => handlePrivilegeToggle(privilege.id)}
                          className="w-full text-left p-4 active:scale-[0.99] transition-transform"
                        >
                          <div className="flex items-center gap-3">
                            {privilege.iconImage ? (
                              <img
                                src={privilege.iconImage}
                                alt={`${privilege.name} icon`}
                                className="w-10 h-10 object-contain"
                              />
                            ) : (
                              <span className="text-3xl">{privilege.icon}</span>
                            )}
                            <div className="flex-1 min-w-0">
                              <div
                                className="font-bold mb-1"
                                style={{ color: privilege.color }}
                              >
                                {privilege.name}
                              </div>
                              <div className="text-[#888888] text-sm">
                                {t.from} {formatPrice(oneMonthTariff.finalPrice)} UZS / {t.perMonth}
                              </div>
                            </div>
                            <ChevronDown
                              className={`w-5 h-5 text-[#888888] transition-transform ${
                                isExpanded ? "rotate-180" : ""
                              }`}
                            />
                          </div>
                        </button>

                        {isExpanded && (
                          <div className="px-4 pb-4 space-y-2">
                            {tariffs.map((tariff) => {
                              const isSelected =
                                selectedOnThisPrivilege &&
                                selectedTariffMonths === tariff.months;
                              const hasDiscount = tariff.fullPrice !== tariff.finalPrice;
                              const cashbackPercent = getPrivilegeCashbackPercent(privilege.id);
                              const cashbackAmount = calculateCashbackAmount(
                                tariff.finalPrice,
                                cashbackPercent,
                              );
                              const isLegendCashback = cashbackPercent >= 10;

                              return (
                                <button
                                  key={`${privilege.id}-${tariff.months}`}
                                  onClick={() => handleTariffSelect(privilege.id, tariff.months)}
                                  className={`w-full flex items-center justify-between rounded-lg border px-3 py-2.5 transition-all active:scale-95 ${
                                    isSelected
                                      ? "bg-[#F08800]/15 border-[#F08800]"
                                      : "bg-[#121212] border-[#2a2a2a] hover:border-[#F08800]/50"
                                  }`}
                                >
                                  <span className="min-w-0">
                                    <span className="text-[#FCFCFC] text-sm font-semibold block">
                                      {tariffLabel(tariff.months, language)}
                                    </span>
                                    <span
                                      className={`inline-flex items-center mt-1 rounded-md px-2 py-1 text-[11px] font-black uppercase tracking-wide ${
                                        isLegendCashback
                                          ? "bg-[#F08800]/22 text-[#FFC26A] border border-[#F08800]/45"
                                          : "bg-[#1f3325] text-[#86efac] border border-[#22c55e]/35"
                                      }`}
                                    >
                                      {t.cashbackLabel} {cashbackPercent}% (+{formatPrice(cashbackAmount)} UZS)
                                    </span>
                                  </span>
                                  <span className="text-sm flex items-center gap-2">
                                    {hasDiscount && (
                                      <span className="text-[#888888] line-through">
                                        {formatPrice(tariff.fullPrice)}
                                      </span>
                                    )}
                                    <span className="text-[#F08800] font-bold">
                                      {formatPrice(tariff.finalPrice)} UZS
                                    </span>
                                  </span>
                                </button>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  })}

                  {showBonusOption && (
                    <div
                      className={`rounded-lg border transition-all ${
                        isBonusFlow
                          ? "bg-[#0D2238] border-[#38BDF8] shadow-lg shadow-[#38BDF8]/15"
                          : "bg-[#121212] border-[#2a2a2a]"
                      }`}
                    >
                      <button
                        onClick={handleBonusToggle}
                        className="w-full text-left p-4 active:scale-[0.99] transition-transform"
                      >
                        <div className="flex items-center gap-3">
                          <div className="w-10 h-10 rounded-lg bg-[#0B3552] border border-[#38BDF8]/40 flex items-center justify-center">
                            <Coins className="w-5 h-5 text-[#38BDF8]" strokeWidth={2.2} />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="font-bold mb-1 text-[#7DD3FC]">{t.bonusTitle}</div>
                            <div className="text-[#9CA3AF] text-xs">{t.bonusSubtitle}</div>
                          </div>
                          <ChevronDown
                            className={`w-5 h-5 text-[#9CA3AF] transition-transform ${
                              isBonusExpanded ? "rotate-180" : ""
                            }`}
                          />
                        </div>
                      </button>

                      {isBonusExpanded && (
                        <div className="px-4 pb-4 space-y-2">
                          {BONUS_TARIFFS.map((tariff) => {
                            const isSelected = selectedBonusTariffId === tariff.id;
                            return (
                              <button
                                key={tariff.id}
                                onClick={() => handleBonusTariffSelect(tariff.id)}
                                className={`w-full flex items-center justify-between rounded-lg border px-3 py-2.5 transition-all active:scale-95 ${
                                  isSelected
                                    ? "bg-[#38BDF8]/15 border-[#38BDF8]"
                                    : "bg-[#121212] border-[#2a2a2a] hover:border-[#38BDF8]/50"
                                }`}
                              >
                                <span className="text-[#E5E7EB] text-sm font-semibold">
                                  {formatPrice(tariff.bonusAmount)} {t.bonusPackage}
                                </span>
                                <span className="text-[#38BDF8] font-bold text-sm">
                                  {formatPrice(tariff.price)} UZS
                                </span>
                              </button>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {currentStep === 3 && (
            <div
              ref={nicknameStepCardRef}
              className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4"
            >
              {isBonusFlow ? (
                <>
                  <div className="flex items-center gap-3 mb-4">
                    <div className="w-10 h-10 bg-[#38BDF8]/15 rounded-lg flex items-center justify-center flex-shrink-0">
                      <Search className="w-5 h-5 text-[#38BDF8]" strokeWidth={2} />
                    </div>
                    <h2 className="text-[#FCFCFC] text-lg">{t.enterSteamId}</h2>
                  </div>

                  {selectedServerData && selectedBonusTariff && (
                    <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                      <div className="text-[#FCFCFC] text-sm font-bold">
                        {formatPrice(selectedBonusTariff.bonusAmount)} {t.bonusPackage}
                      </div>
                      <div className="text-[#38BDF8] text-sm font-bold mt-1">
                        {formatPrice(selectedBonusTariff.price)} UZS
                      </div>
                      <div className="text-[#888888] text-xs mt-1 truncate">
                        {selectedServerData.name}
                      </div>
                    </div>
                  )}

                  <div>
                    <label className="block text-[#888888] text-sm mb-2 font-semibold">
                      {t.steamIdLabel}
                    </label>
                    <input
                      type="text"
                      value={steamId}
                      onChange={(event) => {
                        setSteamId(event.target.value);
                        setSubmissionError(null);
                      }}
                      onFocus={handleNicknameFocus}
                      placeholder={t.steamIdPlaceholder}
                      className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#38BDF8] focus:outline-none transition-colors ${
                        steamId.length > 0 && !steamIdValidation.isValid
                          ? "border-[#ef4444]"
                          : "border-[#2a2a2a]"
                      }`}
                    />

                    <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                      {t.steamIdRules.map((ruleText) => (
                        <li key={ruleText}>{ruleText}</li>
                      ))}
                    </ul>

                    {steamId.length > 0 && !steamIdValidation.isValid && (
                      <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                        {steamIdValidation.errors.map((errorText) => (
                          <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                            {errorText}
                          </p>
                        ))}
                      </div>
                    )}

                    {submissionError && (
                      <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                        <p className="text-[#fca5a5] text-xs leading-relaxed">{submissionError}</p>
                      </div>
                    )}

                    {isResolvingStep3 && (
                      <div className="mt-3 bg-[#38BDF8]/10 border border-[#38BDF8]/40 rounded-lg p-3">
                        <p className="text-[#7DD3FC] text-xs">{t.steamLookupInProgress}</p>
                      </div>
                    )}
                  </div>
                </>
              ) : (
                <>
                  <div className="flex items-center gap-3 mb-4">
                    <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center flex-shrink-0">
                      <User className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                    </div>
                    <h2 className="text-[#FCFCFC] text-lg">
                      {isPrivilegeSteamMode ? t.enterSteamId : t.enterNick}
                    </h2>
                  </div>

                  {selectedServerData && selectedPrivilegeData && selectedTariff && (
                    <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                      <div className="text-[#FCFCFC] text-sm font-bold">
                        {selectedPrivilegeData.name} • {tariffLabel(selectedTariff.months, language)}
                      </div>
                      <div className="text-[#F08800] text-sm font-bold mt-1">
                        {formatPrice(selectedTariff.finalPrice)} UZS
                      </div>
                      <div className="text-[#888888] text-xs mt-1 truncate">
                        {selectedServerData.name}
                      </div>
                    </div>
                  )}

                  <div className="mb-4">
                    <p className="text-[#888888] text-xs mb-2 font-semibold">{t.privilegeAuthTitle}</p>
                    {isSelectedPrivilegeNicknameOnly ? (
                      <div className="space-y-2">
                        <div className="rounded-lg border bg-[#F08800]/15 border-[#F08800] text-[#F8B24E] px-3 py-2.5 text-sm font-bold text-center">
                          {t.privilegeAuthNick}
                        </div>
                        <p className="text-[#9CA3AF] text-xs leading-relaxed">
                          {t.nicknameOnlyPrivilegeRule}
                        </p>
                      </div>
                    ) : (
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          onClick={() => handlePrivilegeIdentifierTypeChange("steam")}
                          className={`rounded-lg border px-3 py-2.5 text-sm font-bold active:scale-95 transition-all ${
                            privilegeIdentifierType === "steam"
                              ? "bg-[#F08800]/15 border-[#F08800] text-[#F8B24E]"
                              : "bg-[#121212] border-[#2a2a2a] text-[#FCFCFC]"
                          }`}
                        >
                          {t.privilegeAuthSteam}
                        </button>
                        <button
                          onClick={() => handlePrivilegeIdentifierTypeChange("nickname")}
                          className={`rounded-lg border px-3 py-2.5 text-sm font-bold active:scale-95 transition-all ${
                            privilegeIdentifierType === "nickname"
                              ? "bg-[#F08800]/15 border-[#F08800] text-[#F8B24E]"
                              : "bg-[#121212] border-[#2a2a2a] text-[#FCFCFC]"
                          }`}
                        >
                          {t.privilegeAuthNick}
                        </button>
                      </div>
                    )}
                  </div>

                  <div>
                    <label className="block text-[#888888] text-sm mb-2 font-semibold">
                      {isPrivilegeSteamMode ? t.steamIdLabel : t.gameNick}
                    </label>
                    {submissionError && (
                      <div className="mb-3 bg-[#ef4444]/20 border-2 border-[#ef4444]/60 rounded-lg p-3">
                        <p className="text-[#fecaca] text-sm font-semibold leading-relaxed">
                          {submissionError}
                        </p>
                      </div>
                    )}
                    {isPrivilegeSteamMode ? (
                      <>
                        <input
                          type="text"
                          value={steamId}
                          onChange={(event) => {
                            setSteamId(event.target.value);
                            setSubmissionError(null);
                          }}
                          onFocus={handleNicknameFocus}
                          placeholder={t.steamIdPlaceholder}
                          className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#F08800] focus:outline-none transition-colors ${
                            steamId.length > 0 && !steamIdValidation.isValid
                              ? "border-[#ef4444]"
                              : "border-[#2a2a2a]"
                          }`}
                        />

                        <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                          {t.steamIdRules.map((ruleText) => (
                            <li key={ruleText}>{ruleText}</li>
                          ))}
                        </ul>

                        {steamId.length > 0 && !steamIdValidation.isValid && (
                          <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                            {steamIdValidation.errors.map((errorText) => (
                              <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                                {errorText}
                              </p>
                            ))}
                          </div>
                        )}
                      </>
                    ) : (
                      <>
                        <input
                          type="text"
                          value={nickname}
                          onChange={(event) => {
                            setNickname(event.target.value);
                            setSubmissionError(null);
                          }}
                          onFocus={handleNicknameFocus}
                          placeholder={t.nickPlaceholder}
                          className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#F08800] focus:outline-none transition-colors ${
                            nickname.length > 0 && !nicknameValidation.isValid
                              ? "border-[#ef4444]"
                              : "border-[#2a2a2a]"
                          }`}
                        />

                        <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                          {t.nickRules.map((ruleText) => (
                            <li key={ruleText}>{ruleText}</li>
                          ))}
                        </ul>

                        {nickname.length > 0 && !nicknameValidation.isValid && (
                          <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                            {nicknameValidation.errors.map((errorText) => (
                              <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                                {errorText}
                              </p>
                            ))}
                          </div>
                        )}
                      </>
                    )}

                    {isResolvingPrivilegeAccount && (
                      <div className="mt-3 bg-[#F08800]/10 border border-[#F08800]/30 rounded-lg p-3">
                        <p className="text-[#F8B24E] text-xs">{t.privilegeLookupInProgress}</p>
                      </div>
                    )}
                  </div>
                </>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div
              ref={passwordStepCardRef}
              className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4"
            >
              {isBonusFlow ? (
                <>
                  <div className="flex items-center gap-3 mb-4">
                    <div className="w-10 h-10 bg-[#38BDF8]/15 rounded-lg flex items-center justify-center flex-shrink-0">
                      <Coins className="w-5 h-5 text-[#38BDF8]" strokeWidth={2} />
                    </div>
                    <h2 className="text-[#FCFCFC] text-lg">{t.bonusConfirmation}</h2>
                  </div>

                  {selectedBonusTariff && (
                    <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                      <div className="text-[#38BDF8] text-sm font-bold">
                        +{formatPrice(selectedBonusTariff.bonusAmount)} {t.bonusPackage}
                      </div>
                      <div className="text-[#FCFCFC] text-xs mt-1">{selectedServerData?.name}</div>
                    </div>
                  )}

                  {bonusAccountInfo && selectedBonusTariff && (
                    <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 space-y-2">
                      <p className="text-[#888888] text-sm">
                        {t.yourNickname}:{" "}
                        <span className="text-[#FCFCFC] font-bold">{bonusAccountInfo.nickname}</span>
                      </p>
                      <p className="text-[#888888] text-sm">
                        {t.yourBonuses}:{" "}
                        <span className="text-[#FCFCFC] font-bold">
                          {formatPrice(bonusAccountInfo.bonusCount)}
                        </span>
                      </p>
                      <p className="text-[#7DD3FC] text-sm leading-relaxed">
                        {t.bonusConfirmQuestionStart}
                        <span className="font-bold">
                          {formatPrice(selectedBonusTariff.bonusAmount)}
                        </span>
                        {t.bonusConfirmQuestionEnd}
                      </p>
                    </div>
                  )}
                </>
              ) : (
                <>
                  <div className="flex items-center gap-3 mb-4">
                    <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center flex-shrink-0">
                      <Lock className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                    </div>
                    <h2 className="text-[#FCFCFC] text-lg">
                      {isPrivilegeSteamMode ? t.enterSteamId : t.setPassword}
                    </h2>
                  </div>

                  <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                    <span className="text-[#888888] text-xs">
                      {isPrivilegeSteamMode ? `${t.steamLabel}:` : t.nicknameLabel}{" "}
                    </span>
                    <span className="text-[#FCFCFC] font-bold text-sm">
                      {isPrivilegeSteamMode ? normalizedSteamId : nickname.trim()}
                    </span>
                  </div>

                  {(isPrivilegeRenewalFlow || isPrivilegePermanentBlocked) ? (
                    <div className="space-y-3">
                      <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3">
                        <p className="text-[#FCFCFC] text-sm font-bold">{t.existingPrivilegeFound}</p>
                        <p className="text-[#9CA3AF] text-xs mt-2">
                          {t.existingPrivilegeType}:{" "}
                          <span className="text-[#FCFCFC] font-semibold">
                            {privilegeAccountInfo?.privilege || "-"}
                          </span>
                        </p>
                        <p className="text-[#9CA3AF] text-xs mt-1">
                          {privilegeAccountInfo?.isPermanent ? (
                            <span className="text-[#FCFCFC] font-semibold">{t.existingPrivilegePermanent}</span>
                          ) : (
                            <>
                              {t.existingPrivilegeDays}:{" "}
                              <span className="text-[#FCFCFC] font-semibold">
                                {formatPrice(privilegeAccountInfo?.days ?? 0)}
                              </span>
                            </>
                          )}
                        </p>
                        <p className="text-[#9CA3AF] text-xs mt-1">
                          {t.selectedPrivilegeType}:{" "}
                          <span className="text-[#FCFCFC] font-semibold">
                            {selectedPrivilegeData?.name ?? "-"}
                          </span>
                        </p>
                        {privilegeAccountInfo?.isDisabled && (
                          <p className="text-[#FCA5A5] text-xs mt-1">{t.existingPrivilegeDisabled}</p>
                        )}
                      </div>

                      {privilegePaymentAdjustment.isUpgradeWithCredit && (
                        <div className="bg-[#16a34a]/10 border border-[#22c55e]/40 rounded-lg p-3">
                          <p className="text-[#86efac] text-xs">{t.upgradeCreditApplied}</p>
                          <p className="text-[#d1fae5] text-xs mt-1">
                            {t.upgradeCreditAmount}:{" "}
                            <span className="font-semibold">
                              {formatPrice(privilegePaymentAdjustment.creditAmount)} UZS
                            </span>
                          </p>
                          <p className="text-[#d1fae5] text-xs mt-1">
                            {t.recalculatedAmount}:{" "}
                            <span className="font-semibold">{formatPrice(selectedPrice)} UZS</span>
                          </p>
                        </div>
                      )}

                      {isPrivilegePermanentBlocked ? (
                        <div className="bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                          <p className="text-[#fca5a5] text-xs leading-relaxed">{t.permanentPrivilegeBlocked}</p>
                        </div>
                      ) : isPrivilegeDowngradeBlocked ? (
                        <div className="bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                          <p className="text-[#fca5a5] text-xs leading-relaxed">{t.downgradeBlocked}</p>
                        </div>
                      ) : !renewalRequested ? (
                        <div className="space-y-2">
                          <p className="text-[#FCFCFC] text-sm">{t.renewQuestion}</p>
                          <div className="grid grid-cols-2 gap-2">
                            <button
                              onClick={() => {
                                setRenewalRequested(true);
                                setSubmissionError(null);
                              }}
                              className="bg-[#F08800] text-[#121212] font-bold rounded-lg py-2.5 text-sm active:scale-95"
                            >
                              {t.renewYes}
                            </button>
                            <button
                              onClick={() => {
                                setRenewalRequested(false);
                                setCurrentPasswordVerified(false);
                                setCurrentPassword("");
                                setChangePasswordChoice(null);
                                setNewPassword("");
                                setCurrentStep(3);
                              }}
                              className="bg-[#2a2a2a] text-[#FCFCFC] font-bold rounded-lg py-2.5 text-sm active:scale-95"
                            >
                              {t.renewNo}
                            </button>
                          </div>
                        </div>
                      ) : isPrivilegeSteamMode ? (
                        <div className="bg-[#0B3552]/20 border border-[#38BDF8]/40 rounded-lg p-3">
                          <p className="text-[#7DD3FC] text-xs leading-relaxed">
                            {isUz
                              ? "STEAM_ID rejimida parol tekshiruvi talab qilinmaydi. To'lov tasdiqlangach muddat qo'shiladi."
                              : "В режиме STEAM_ID проверка пароля не требуется. После подтверждения оплаты срок будет продлён."}
                          </p>
                        </div>
                      ) : (
                        <div className="space-y-3">
                          <div>
                            <label className="block text-[#888888] text-sm mb-2 font-semibold">
                              {t.currentPasswordLabel}
                            </label>
                            <input
                              type="text"
                              value={currentPassword}
                              onChange={(event) => {
                                setCurrentPassword(event.target.value);
                                setCurrentPasswordVerified(false);
                                setSubmissionError(null);
                              }}
                              onFocus={handlePasswordFocus}
                              placeholder={t.currentPasswordPlaceholder}
                              className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#F08800] focus:outline-none transition-colors ${
                                currentPassword.length > 0 && !currentPasswordValidation.isValid
                                  ? "border-[#ef4444]"
                                  : "border-[#2a2a2a]"
                              }`}
                            />

                            <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                              {t.passwordRules.map((ruleText) => (
                                <li key={`renew-${ruleText}`}>{ruleText}</li>
                              ))}
                            </ul>

                            {currentPassword.length > 0 && !currentPasswordValidation.isValid && (
                              <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                                {currentPasswordValidation.errors.map((errorText) => (
                                  <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                                    {errorText}
                                  </p>
                                ))}
                              </div>
                            )}
                          </div>

                          {isVerifyingCurrentPassword && (
                            <div className="bg-[#F08800]/10 border border-[#F08800]/30 rounded-lg p-3">
                              <p className="text-[#F8B24E] text-xs">{t.currentPasswordCheck}</p>
                            </div>
                          )}

                          {currentPasswordVerified && (
                            <div className="bg-green-500/10 border border-green-500/40 rounded-lg p-3">
                              <p className="text-green-300 text-xs">{t.currentPasswordVerified}</p>
                            </div>
                          )}

                          {currentPasswordVerified && (
                            <div className="space-y-2">
                              <p className="text-[#FCFCFC] text-sm">{t.changePasswordQuestion}</p>
                              <div className="grid grid-cols-2 gap-2">
                                <button
                                  onClick={() => setChangePasswordChoice(true)}
                                  className={`rounded-lg py-2.5 text-sm font-bold active:scale-95 ${
                                    changePasswordChoice === true
                                      ? "bg-[#F08800] text-[#121212]"
                                      : "bg-[#2a2a2a] text-[#FCFCFC]"
                                  }`}
                                >
                                  {t.changePasswordYes}
                                </button>
                                <button
                                  onClick={() => {
                                    setChangePasswordChoice(false);
                                    setNewPassword("");
                                  }}
                                  className={`rounded-lg py-2.5 text-sm font-bold active:scale-95 ${
                                    changePasswordChoice === false
                                      ? "bg-[#F08800] text-[#121212]"
                                      : "bg-[#2a2a2a] text-[#FCFCFC]"
                                  }`}
                                >
                                  {t.changePasswordNo}
                                </button>
                              </div>
                            </div>
                          )}

                          {currentPasswordVerified && changePasswordChoice === true && (
                            <div>
                              <label className="block text-[#888888] text-sm mb-2 font-semibold">
                                {t.newPasswordLabel}
                              </label>
                              <input
                                type="text"
                                value={newPassword}
                                onChange={(event) => setNewPassword(event.target.value)}
                                onFocus={handlePasswordFocus}
                                placeholder={t.newPasswordPlaceholder}
                                className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#F08800] focus:outline-none transition-colors ${
                                  newPassword.length > 0 && !newPasswordValidation.isValid
                                    ? "border-[#ef4444]"
                                    : "border-[#2a2a2a]"
                                }`}
                              />

                              <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                                {t.passwordRules.map((ruleText) => (
                                  <li key={`new-${ruleText}`}>{ruleText}</li>
                                ))}
                              </ul>

                              {newPassword.length > 0 && !newPasswordValidation.isValid && (
                                <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                                  {newPasswordValidation.errors.map((errorText) => (
                                    <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                                      {errorText}
                                    </p>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ) : (
                    isPrivilegeSteamMode ? (
                      <div className="bg-[#0B3552]/20 border border-[#38BDF8]/40 rounded-lg p-3">
                        <p className="text-[#7DD3FC] text-xs leading-relaxed">
                          {isUz
                            ? "Imtiyoz to'g'ridan-to'g'ri STEAM_ID bo'yicha beriladi. Parol kiritish talab qilinmaydi."
                            : "Привилегия будет выдана напрямую на STEAM_ID. Ввод пароля не требуется."}
                        </p>
                      </div>
                    ) : (
                      <div>
                        <label className="block text-[#888888] text-sm mb-2 font-semibold">
                          {t.passwordLabel}
                        </label>
                        <input
                          type="text"
                          value={password}
                          onChange={(event) => setPassword(event.target.value)}
                          onFocus={handlePasswordFocus}
                          placeholder={t.passwordPlaceholder}
                          className={`w-full bg-[#121212] border-2 rounded-lg px-4 py-3 text-[#FCFCFC] focus:border-[#F08800] focus:outline-none transition-colors ${
                            password.length > 0 && !passwordValidation.isValid
                              ? "border-[#ef4444]"
                              : "border-[#2a2a2a]"
                          }`}
                        />

                        <ul className="text-[#888888] text-xs mt-3 list-disc pl-5 space-y-1">
                          {t.passwordRules.map((ruleText) => (
                            <li key={ruleText}>{ruleText}</li>
                          ))}
                        </ul>

                        {password.length > 0 && !passwordValidation.isValid && (
                          <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                            {passwordValidation.errors.map((errorText) => (
                              <p key={errorText} className="text-[#fca5a5] text-xs leading-relaxed">
                                {errorText}
                              </p>
                            ))}
                          </div>
                        )}
                      </div>
                    )
                  )}

                  {submissionError && (
                    <div className="mt-3 bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3">
                      <p className="text-[#fca5a5] text-xs leading-relaxed">{submissionError}</p>
                    </div>
                  )}
                </>
              )}
            </div>
          )}

          {currentStep === 5 && paymentStatus === "idle" && (
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="flex items-center gap-3 mb-4">
                <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center flex-shrink-0">
                  <Coins className="w-5 h-5 text-[#F08800]" strokeWidth={2} />
                </div>
                <h2 className="text-[#FCFCFC] text-lg">{isUz ? "Xaridni tasdiqlash" : "Подтверждение покупки"}</h2>
              </div>

              {submissionError && (
                <div className="mb-4 bg-[#7f1d1d]/50 border-2 border-[#ef4444]/80 rounded-lg p-3.5 shadow-[0_0_24px_rgba(239,68,68,0.18)]">
                  <p className="text-[#fecaca] text-sm font-semibold leading-relaxed">
                    {submissionError}
                  </p>
                </div>
              )}

              {selectedServerData && selectedProductType && (
                <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-3 mb-4">
                  <div className="text-[#888888] text-xs">{t.amountToPay}</div>
                  <div className={`font-bold text-lg mt-1 ${isBonusFlow ? "text-[#38BDF8]" : "text-[#F08800]"}`}>
                    {formatPrice(selectedPrice)} UZS
                  </div>
                  <div className="text-[#FCFCFC] text-sm mt-2">
                    {isBonusFlow && selectedBonusTariff
                      ? `${formatPrice(selectedBonusTariff.bonusAmount)} ${t.bonusPackage}`
                      : selectedPrivilegeData && selectedTariff
                        ? `${selectedPrivilegeData.name} • ${tariffLabel(selectedTariff.months, language)}`
                        : ""}
                  </div>
                  {!isBonusFlow && selectedPrivilegeCashbackAmount > 0 && (
                    <div className="mt-2 inline-flex items-center rounded-md px-2 py-1 border border-[#22c55e]/35 bg-[#1f3325]">
                      <span className="text-[#86efac] text-[11px] font-black uppercase tracking-wide">
                        {t.cashbackWillReturn}: {selectedPrivilegeCashbackPercent}% (+{formatPrice(selectedPrivilegeCashbackAmount)} UZS)
                      </span>
                    </div>
                  )}
                  <div className="text-[#888888] text-xs mt-1 truncate">
                    {selectedServerData.name}
                  </div>
                </div>
              )}

              <div
                className={`border rounded-lg p-3 mb-4 ${
                  hasEnoughBalance
                    ? "bg-[#0f2916] border-[#22c55e]/40"
                    : "bg-[#3b2004] border-[#F08800]/45"
                }`}
              >
                {hasEnoughBalance ? (
                  <p className="text-[#86efac] text-sm font-semibold leading-relaxed">
                    {t.enoughBalanceHint}
                  </p>
                ) : (
                  <div className="space-y-2">
                    <p className="text-[#f5c983] text-sm font-semibold">{t.insufficientBalanceHint}</p>
                    <p className="text-[#FCFCFC] text-sm">
                      {t.missingAmountLabel}:{" "}
                      <span className="text-[#F8B24E] font-black">
                        {formatBalanceMoney(missingBalanceAmount)} UZS
                      </span>
                    </p>
                    <button
                      type="button"
                      onClick={openTopUp}
                      className="inline-flex items-center gap-2 bg-[#121212] border border-[#F08800]/55 rounded-lg px-3 py-2 text-[#FCFCFC] text-xs font-bold uppercase tracking-wide"
                    >
                      <PlusCircle className="w-4 h-4 text-[#F08800]" strokeWidth={2.2} />
                      {t.topUpBalance}
                    </button>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 5 && paymentStatus !== "idle" && (
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <div className="bg-[#121212] border border-[#2a2a2a] rounded-lg p-5">
                <div className="flex items-center gap-4 mb-4">
                  {paymentStatus === "processing" ? (
                    <img
                      src={strikeMarkLogo}
                      alt="Strike.Uz processing logo"
                      className="h-12 w-auto strike-wait-float object-contain"
                    />
                  ) : (
                    <div className="w-14 h-14 rounded-full bg-green-500/20 border border-green-500/40 flex items-center justify-center">
                      <Check className="w-8 h-8 text-green-500" strokeWidth={3} />
                    </div>
                  )}

                  <div>
                    <h3 className="text-[#FCFCFC] text-base font-bold">
                      {paymentStatus === "processing"
                        ? t.processing
                        : isBonusFlow
                          ? t.issuedBonus
                          : t.issuedPrivilege}
                    </h3>
                    {paymentStatus === "processing" ? (
                      <p className="text-[#888888] text-xs mt-1 leading-relaxed">
                        {isBonusFlow ? (
                          t.processingBonusText
                        ) : (
                          <>
                            {t.processingText}
                            <span className="text-[#F08800] font-bold">
                              {selectedPrivilegeData?.name}
                            </span>
                            {t.processingTextMiddle}
                            <span className="text-[#FCFCFC]">{selectedServerData?.name}</span>
                            {isPrivilegeSteamMode ? t.processingTextSteamSuffix : t.processingTextSuffix}
                            <span className="text-[#FCFCFC]">
                              {isPrivilegeSteamMode ? normalizedSteamId : nickname.trim()}
                            </span>.
                          </>
                        )}
                      </p>
                    ) : (
                      <div className="text-[#888888] text-xs mt-1 leading-relaxed">
                        {isBonusFlow ? (
                          <>
                            <p>
                              {t.serverLabel}:{" "}
                              <span className="text-[#FCFCFC]">{selectedServerData?.name}</span>
                            </p>
                            <p className="mt-1">
                              STEAM_ID: <span className="text-[#FCFCFC]">{normalizedSteamId}</span>
                            </p>
                            <p className="mt-1">
                              {t.yourNickname}:{" "}
                              <span className="text-[#FCFCFC]">
                                {purchaseResponse?.bonusResult?.nickname ?? bonusAccountInfo?.nickname ?? "-"}
                              </span>
                            </p>
                            <p className="mt-1">
                              {t.yourBonuses}:{" "}
                              <span className="text-[#FCFCFC]">
                                {formatPrice(
                                  purchaseResponse?.bonusResult?.after ??
                                    bonusAccountInfo?.bonusCount ??
                                    0,
                                )}
                              </span>
                            </p>
                          </>
                        ) : (
                          <>
                            <p>
                              {t.serverLabel}:{" "}
                              <span className="text-[#FCFCFC]">{selectedServerData?.name}</span>
                            </p>
                            <p className="mt-1">
                              {isPrivilegeSteamMode ? t.steamLabel : t.nickLabel}:{" "}
                              <span className="text-[#FCFCFC]">
                                {isPrivilegeSteamMode ? normalizedSteamId : nickname.trim()}
                              </span>
                            </p>
                            {Number(purchaseResponse?.cashback?.amount || 0) > 0 && (
                              <p className="mt-1 text-[#86efac]">
                                {t.cashbackCredited}:{" "}
                                <span className="text-[#FCFCFC]">
                                  +{formatBalanceMoney(Number(purchaseResponse?.cashback?.amount || 0))} UZS
                                </span>
                              </p>
                            )}
                          </>
                        )}
                      </div>
                    )}
                  </div>
                </div>

                {!isBonusFlow && !isPrivilegeSteamMode && paymentStatus === "success" && (
                  <div className="bg-[#0f0f0f] border border-[#2a2a2a] rounded-lg p-3">
                    <p className="text-[#888888] text-xs mb-2">
                      {t.consoleInstruction}
                    </p>

                    <div className="flex items-center gap-2 bg-[#121212] rounded-lg p-3">
                      <span className="text-[#FCFCFC] font-mono text-xs flex-1 break-all">
                        {passwordCommand}
                      </span>
                      <button
                        onClick={handleCopyPasswordCommand}
                        className="p-2 text-[#888888] hover:text-[#F08800] hover:bg-[#2a2a2a] rounded transition-colors flex-shrink-0 active:scale-95"
                      >
                        {passwordCopied ? (
                          <span className="text-green-500 text-xs font-bold px-1">✓</span>
                        ) : (
                          <Copy className="w-4 h-4" strokeWidth={2} />
                        )}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {paymentStatus === "idle" && currentStep > 1 && (
            <div ref={actionsContainerRef} className="flex gap-3">
              {!(currentStep === 5 && isPaymentSessionActive) && (
                <button
                  onClick={handleBackStep}
                  className="flex-1 bg-[#2a2a2a] hover:bg-[#3a3a3a] text-[#FCFCFC] font-black py-4 rounded-lg transition-all active:scale-95 uppercase tracking-wide text-sm"
                >
                  {t.back}
                </button>
              )}

              {currentStep < 5 ? (
                <button
                  onClick={() => void handleNextStep()}
                  disabled={
                    isResolvingStep3 ||
                    isResolvingPrivilegeAccount ||
                    (currentStep === 2 && !canProceedStep2) ||
                    (currentStep === 3 && !canProceedStep3) ||
                    (currentStep === 4 && !canProceedStep4)
                  }
                  className={`flex-1 font-black py-4 rounded-lg transition-all uppercase tracking-wide text-sm ${
                    !isResolvingStep3 &&
                    !isResolvingPrivilegeAccount &&
                    ((currentStep === 2 && canProceedStep2) ||
                      (currentStep === 3 && canProceedStep3) ||
                      (currentStep === 4 && canProceedStep4))
                      ? "bg-[#F08800] hover:bg-[#d97700] text-[#121212] shadow-lg shadow-[#F08800]/30 active:scale-95"
                      : "bg-[#2a2a2a] text-[#555555] cursor-not-allowed"
                  }`}
                >
                  {isResolvingStep3
                    ? t.steamLookupInProgress
                    : isResolvingPrivilegeAccount
                      ? t.privilegeLookupInProgress
                      : t.next}
                </button>
              ) : (
                <button
                  onClick={() => void handleSubmit()}
                  disabled={!canProceedStep5}
                  className={`font-black py-4 rounded-lg transition-all uppercase tracking-wide text-sm ${
                    currentStep === 5 && isPaymentSessionActive ? "w-full" : "flex-1"
                  } ${
                    canProceedStep5
                      ? "bg-[#F08800] hover:bg-[#d97700] text-[#121212] shadow-lg shadow-[#F08800]/30 active:scale-95"
                      : "bg-[#2a2a2a] text-[#555555] cursor-not-allowed"
                  }`}
                >
                  {isBonusFlow ? t.buyBonus : t.buyPrivilege}
                </button>
              )}
            </div>
          )}

          {paymentStatus === "idle" && submissionError && currentStep !== 3 && currentStep !== 5 && (
            <div className="bg-[#ef4444]/10 border border-[#ef4444]/40 rounded-lg p-3.5">
              <p className="text-[#fca5a5] text-xs text-center leading-relaxed">
                {submissionError}
              </p>
            </div>
          )}

          {paymentStatus === "idle" && (
            <div className="bg-[#F08800]/10 border border-[#F08800]/30 rounded-lg p-3.5">
              <p className="text-[#888888] text-xs text-center leading-relaxed">
                {isBonusFlow ? t.pendingBonusNote : t.pendingPrivilegeNote}
              </p>
            </div>
          )}
          </>
          )}
        </div>
      </div>
      {cashbackToast && (
        <div className="fixed left-3 right-3 bottom-[calc(5.3rem+env(safe-area-inset-bottom))] z-[130] max-w-[460px] mx-auto">
          <div className="rounded-xl border border-[#22c55e]/45 bg-gradient-to-r from-[#0d2b18] via-[#124124] to-[#0f2f1b] px-4 py-3 shadow-[0_0_32px_rgba(34,197,94,0.35)]">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[#86efac] text-xs font-black uppercase tracking-[0.12em]">
                  {t.cashbackToastTitle}
                </p>
                <p className="text-[#dcfce7] text-sm font-bold mt-1">
                  +{formatBalanceMoney(cashbackToast.amount)} UZS ({cashbackToast.percent}%)
                </p>
              </div>
              <button
                type="button"
                onClick={() => setCashbackToast(null)}
                className="rounded-md border border-[#22c55e]/35 bg-[#0d2014] px-2 py-1 text-[10px] text-[#bbf7d0] font-bold uppercase tracking-wide"
              >
                {isUz ? "Yopish" : "Закрыть"}
              </button>
            </div>
          </div>
        </div>
      )}
    </PageTransition>
  );
}
