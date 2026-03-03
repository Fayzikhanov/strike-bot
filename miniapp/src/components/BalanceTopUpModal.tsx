import { useEffect, useMemo, useState, type ChangeEvent } from "react";
import { Check, Copy, Loader2, Upload, WalletCards, X } from "lucide-react";
import { submitBalanceTopUp, type BalanceTopUpResponse } from "../api/strikeApi";
import {
  clearPersistedTopUpFlow,
  isTopUpUploadSessionActive,
  readPersistedTopUpFlowForUser,
  savePersistedTopUpFlow,
  type TopUpFlowStep,
  type TopUpUploadSession,
} from "../lib/balanceTopUpFlow";

interface BalanceTopUpModalProps {
  isOpen: boolean;
  onClose: () => void;
  userId: number;
  language: "ru" | "uz";
  onSuccess?: (response: BalanceTopUpResponse) => void;
}

type ModalStep = 1 | 2 | 3;

const TOPUP_SESSION_SECONDS = 5 * 60;
const PAYMENT_CARD_NUMBER = "9860 1001 2447 4881";
const PAYMENT_RECIPIENT = "Fayzixanov M.";

function formatCountdown(seconds: number): string {
  const safe = Math.max(Math.floor(seconds), 0);
  const mins = Math.floor(safe / 60);
  const secs = safe % 60;
  return `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result === "string" && reader.result.startsWith("data:")) {
        resolve(reader.result);
        return;
      }
      reject(new Error("Invalid file data"));
    };
    reader.onerror = () => reject(new Error("Failed to read file"));
    reader.readAsDataURL(file);
  });
}

function extractReadableError(error: unknown): string {
  if (error instanceof Error) {
    const text = error.message.trim();
    if (text) {
      return text;
    }
  }
  if (typeof error === "string") {
    const text = error.trim();
    if (text) {
      return text;
    }
  }
  return "";
}

function stepToFlowStep(step: ModalStep): TopUpFlowStep {
  if (step === 2) {
    return "card";
  }
  if (step === 3) {
    return "upload";
  }
  return "intro";
}

function flowStepToModalStep(step: TopUpFlowStep): ModalStep {
  if (step === "card") {
    return 2;
  }
  if (step === "upload") {
    return 3;
  }
  return 1;
}

export function BalanceTopUpModal({
  isOpen,
  onClose,
  userId,
  language,
  onSuccess,
}: BalanceTopUpModalProps) {
  const isUz = language === "uz";
  const [step, setStep] = useState<ModalStep>(1);
  const [session, setSession] = useState<TopUpUploadSession | null>(null);
  const [secondsLeft, setSecondsLeft] = useState(0);
  const [screenshot, setScreenshot] = useState<File | null>(null);
  const [errorText, setErrorText] = useState<string | null>(null);
  const [copyHint, setCopyHint] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const t = useMemo(
    () => ({
      title: isUz ? "Balansni to'ldirish" : "Пополнение баланса",
      step: isUz ? "Bosqich" : "Шаг",
      of: isUz ? "dan" : "из",
      close: isUz ? "Yopish" : "Закрыть",
      next: isUz ? "Davom etish" : "Далее",
      back: isUz ? "Orqaga" : "Назад",
      copyCardTitle: isUz
        ? "To'lov uchun karta raqamini nusxalang"
        : "Скопируйте номер карты для перевода",
      copyCardHint: isUz
        ? "Har qanday qulay to'lov ilovasidan ushbu karta raqamiga o'tkazma qiling."
        : "Сделайте перевод на этот номер карты через любое удобное приложение.",
      recipient: isUz ? "Qabul qiluvchi" : "Получатель",
      copy: isUz ? "Nusxalash" : "Скопировать",
      copied: isUz ? "Karta raqami nusxalandi" : "Номер карты успешно скопирован",
      toUploadStep: isUz ? "Skrinshot yuborishga o'tish" : "Перейти к отправке скриншота",
      uploadTitle: isUz ? "To'lov skrinshotini yuboring" : "Отправьте скриншот оплаты",
      uploadHint: isUz
        ? "Yuqoridagi karta raqamiga to'ldirmoqchi bo'lgan summani o'tkazing va to'lov tasdiq skrinshotini yuboring."
        : "Переведите на карту выше сумму, которую хотите зачислить на баланс, и отправьте скриншот подтверждения.",
      timer: isUz ? "To'lov oynasi" : "Окно оплаты",
      chooseFile: isUz ? "Skrinshot tanlang" : "Выберите скриншот",
      fileHint: isUz ? "PNG/JPG, 10MB gacha" : "PNG/JPG, до 10MB",
      submit: isUz ? "Skrinshotni yuborish" : "Отправить скриншот",
      wait: isUz ? "Tekshirilmoqda..." : "Проверяем...",
      missingFile: isUz ? "Avval skrinshot yuklang." : "Сначала загрузите скриншот.",
      sessionExpired: isUz
        ? "5 daqiqalik sessiya tugadi. Qayta boshlang."
        : "5-минутная сессия истекла. Начните заново.",
      restartSession: isUz ? "Qayta boshlash" : "Начать заново",
      lockHint: isUz
        ? "Taymer tugamaguncha bu oynani yopib bo'lmaydi."
        : "Пока таймер не закончится, это окно закрыть нельзя.",
      sendFailed: isUz
        ? "Popolnenie yuborilmadi. Qayta urinib ko'ring."
        : "Не удалось отправить пополнение. Попробуйте снова.",
      step1Title: isUz ? "Qanday ishlaydi?" : "Как это работает?",
      step1Text1: isUz
        ? "1) Karta ma'lumotlarini olasiz va to'lov qilasiz."
        : "1) Получаете данные карты и делаете перевод.",
      step1Text2: isUz
        ? "2) Tasdiq skrinshotini yuborasiz."
        : "2) Отправляете скриншот подтверждения.",
      step1Text3: isUz
        ? "3) Bot to'lovni tekshiradi va summani balansga qo'shadi."
        : "3) Бот проверяет оплату и зачисляет сумму на баланс.",
    }),
    [isUz],
  );

  const isUploadStep = step === 3;
  const isSessionLocked = isUploadStep && !!session && secondsLeft > 0;

  const persistFlowState = (nextStep: ModalStep, nextSession: TopUpUploadSession | null) => {
    if (userId <= 0) {
      return;
    }
    savePersistedTopUpFlow({
      version: 1,
      userId,
      step: stepToFlowStep(nextStep),
      session: nextSession,
      updatedAt: Math.floor(Date.now() / 1000),
    });
  };

  const resetTransientState = () => {
    setStep(1);
    setSession(null);
    setSecondsLeft(0);
    setScreenshot(null);
    setErrorText(null);
    setCopyHint(null);
    setIsSubmitting(false);
  };

  useEffect(() => {
    if (!isOpen || userId <= 0) {
      return;
    }

    const persisted = readPersistedTopUpFlowForUser(userId);
    if (persisted && isTopUpUploadSessionActive(persisted) && persisted.session) {
      const restoredStep = flowStepToModalStep(persisted.step);
      const now = Math.floor(Date.now() / 1000);
      setStep(restoredStep);
      setSession(persisted.session);
      setSecondsLeft(Math.max(persisted.session.expiresAt - now, 0));
      setScreenshot(null);
      setErrorText(null);
      setCopyHint(null);
      setIsSubmitting(false);
      return;
    }

    clearPersistedTopUpFlow();
    resetTransientState();
    persistFlowState(1, null);
  }, [isOpen, userId]);

  useEffect(() => {
    if (!isOpen || userId <= 0) {
      return;
    }

    const persisted = readPersistedTopUpFlowForUser(userId);
    if (!persisted || !isTopUpUploadSessionActive(persisted)) {
      return;
    }

    if (step !== 3 || !session) {
      return;
    }

    const refreshState = () => {
      const latest = readPersistedTopUpFlowForUser(userId);
      if (!latest || !isTopUpUploadSessionActive(latest) || !latest.session) {
        return;
      }
      setSession(latest.session);
      const now = Math.floor(Date.now() / 1000);
      setSecondsLeft(Math.max(latest.session.expiresAt - now, 0));
    };

    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        refreshState();
      }
    };

    window.addEventListener("focus", refreshState);
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      window.removeEventListener("focus", refreshState);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [isOpen, session, step, userId]);

  useEffect(() => {
    if (!isOpen || step !== 3 || !session) {
      return;
    }

    const update = () => {
      const now = Math.floor(Date.now() / 1000);
      const remaining = Math.max(session.expiresAt - now, 0);
      setSecondsLeft(remaining);

      if (remaining <= 0) {
        clearPersistedTopUpFlow();
        setSession(null);
        setErrorText((current) => current || t.sessionExpired);
      }
    };

    update();
    const timerId = window.setInterval(update, 1000);
    return () => window.clearInterval(timerId);
  }, [isOpen, session, step, t.sessionExpired]);

  useEffect(() => {
    if (!copyHint) {
      return;
    }
    const timerId = window.setTimeout(() => setCopyHint(null), 2200);
    return () => window.clearTimeout(timerId);
  }, [copyHint]);

  if (!isOpen) {
    return null;
  }

  const handleClose = () => {
    if (isSessionLocked) {
      return;
    }
    clearPersistedTopUpFlow();
    resetTransientState();
    onClose();
  };

  const handleCopyCard = async () => {
    try {
      await navigator.clipboard.writeText(PAYMENT_CARD_NUMBER.replace(/\s+/g, ""));
      setCopyHint(t.copied);
    } catch {
      setCopyHint(t.copied);
    }
  };

  const handleNextFromIntro = () => {
    setErrorText(null);
    setStep(2);
    persistFlowState(2, null);
  };

  const handleBackToIntro = () => {
    if (isSubmitting) {
      return;
    }
    setErrorText(null);
    setStep(1);
    persistFlowState(1, null);
  };

  const handleStartUploadStep = () => {
    if (userId <= 0) {
      return;
    }
    const startedAt = Math.floor(Date.now() / 1000);
    const nextSession: TopUpUploadSession = {
      sessionId: `${userId}-topup-${Math.random().toString(36).slice(2, 9)}-${startedAt}`,
      startedAt,
      expiresAt: startedAt + TOPUP_SESSION_SECONDS,
    };
    setStep(3);
    setSession(nextSession);
    setSecondsLeft(TOPUP_SESSION_SECONDS);
    setScreenshot(null);
    setErrorText(null);
    persistFlowState(3, nextSession);
  };

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || !event.target.files[0]) {
      return;
    }
    setScreenshot(event.target.files[0]);
    setErrorText(null);
  };

  const handleRestartSession = () => {
    setStep(1);
    setSession(null);
    setSecondsLeft(0);
    setScreenshot(null);
    setErrorText(null);
    setIsSubmitting(false);
    persistFlowState(1, null);
  };

  const handleSubmit = async () => {
    if (!session || secondsLeft <= 0) {
      setErrorText(t.sessionExpired);
      return;
    }
    if (!screenshot) {
      setErrorText(t.missingFile);
      return;
    }

    setIsSubmitting(true);
    setErrorText(null);

    try {
      const screenshotDataUrl = await readFileAsDataUrl(screenshot);
      const telegramUser = (
        window as Window & {
          Telegram?: {
            WebApp?: {
              initDataUnsafe?: {
                user?: {
                  username?: string;
                  first_name?: string;
                  last_name?: string;
                };
              };
            };
          };
        }
      ).Telegram?.WebApp?.initDataUnsafe?.user;
      const response = await submitBalanceTopUp({
        userId,
        username: telegramUser?.username ?? "",
        firstName: telegramUser?.first_name ?? "",
        lastName: telegramUser?.last_name ?? "",
        screenshotDataUrl,
        screenshotName: screenshot.name || "balance-topup.jpg",
        screenshotMimeType: screenshot.type || "image/jpeg",
        topupSessionId: session.sessionId,
        topupSessionStartedAt: session.startedAt,
        topupSessionExpiresAt: session.expiresAt,
        language,
      });
      clearPersistedTopUpFlow();
      resetTransientState();
      onSuccess?.(response);
      onClose();
    } catch (error) {
      const message = extractReadableError(error);
      setErrorText(message || t.sendFailed);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[120] bg-black/72 backdrop-blur-[2px] flex items-end sm:items-center justify-center">
      <div className="w-full max-w-[480px] bg-[#141414] border border-[#2a2a2a] rounded-t-2xl sm:rounded-2xl p-4 sm:p-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h3 className="text-[#FCFCFC] text-lg font-black">{t.title}</h3>
            <p className="text-[#888888] text-xs mt-1">
              {t.step} {step} {t.of} 3
            </p>
          </div>
          <button
            type="button"
            onClick={handleClose}
            disabled={isSessionLocked}
            className={`w-9 h-9 rounded-lg border flex items-center justify-center transition-colors ${
              isSessionLocked
                ? "bg-[#1b1b1b] border-[#2a2a2a] text-[#555555] cursor-not-allowed"
                : "bg-[#1f1f1f] border-[#2a2a2a] text-[#888888] hover:text-[#FCFCFC]"
            }`}
            aria-label={t.close}
          >
            <X className="w-4 h-4" strokeWidth={2.3} />
          </button>
        </div>

        <div className="mt-4 mb-4">
          <div className="flex items-center justify-between">
            {[1, 2, 3].map((stepIndex) => (
              <div key={stepIndex} className="flex items-center">
                <div
                  className={`w-8 h-8 rounded-full flex items-center justify-center font-black text-sm ${
                    stepIndex < step
                      ? "bg-green-500 text-white"
                      : stepIndex === step
                        ? "bg-[#F08800] text-[#121212]"
                        : "bg-[#2a2a2a] text-[#888888]"
                  }`}
                >
                  {stepIndex < step ? <Check className="w-4 h-4" strokeWidth={3} /> : stepIndex}
                </div>
                {stepIndex < 3 && (
                  <div className={`w-10 h-0.5 mx-1 ${stepIndex < step ? "bg-green-500" : "bg-[#2a2a2a]"}`} />
                )}
              </div>
            ))}
          </div>
        </div>

        {step === 1 && (
          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 space-y-3">
            <h4 className="text-[#FCFCFC] text-base font-bold">{t.step1Title}</h4>
            <p className="text-[#B8B8B8] text-sm leading-relaxed">{t.step1Text1}</p>
            <p className="text-[#B8B8B8] text-sm leading-relaxed">{t.step1Text2}</p>
            <p className="text-[#B8B8B8] text-sm leading-relaxed">{t.step1Text3}</p>
          </div>
        )}

        {step === 2 && (
          <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 space-y-3.5">
            <p className="text-[#FCFCFC] text-sm leading-relaxed font-semibold">{t.copyCardTitle}</p>
            <p className="text-[#888888] text-xs leading-relaxed">{t.copyCardHint}</p>
            <div className="bg-[#0f0f0f] border border-[#2a2a2a] rounded-lg p-4">
              <p className="text-[#888888] text-xs mb-2">{t.recipient}: {PAYMENT_RECIPIENT}</p>
              <p className="text-[#FCFCFC] text-[clamp(1.45rem,6.2vw,1.85rem)] font-black tracking-[0.04em] leading-tight whitespace-nowrap text-center overflow-x-auto">
                {PAYMENT_CARD_NUMBER}
              </p>
              <div className="mt-3 flex justify-center">
                <button
                  type="button"
                  onClick={handleCopyCard}
                  className="shrink-0 inline-flex items-center gap-1.5 bg-[#1a1a1a] border border-[#F08800]/55 rounded-lg px-3.5 py-2 text-[#FCFCFC] text-xs font-bold uppercase"
                >
                  <Copy className="w-3.5 h-3.5 text-[#F08800]" strokeWidth={2.2} />
                  {t.copy}
                </button>
              </div>
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-3.5">
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <h4 className="text-[#FCFCFC] text-base font-bold">{t.uploadTitle}</h4>
              <p className="text-[#888888] text-xs mt-1.5 leading-relaxed">{t.uploadHint}</p>

              <div className="mt-3 bg-[#0f0f0f] border border-[#2a2a2a] rounded-lg px-3 py-2.5 flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <p className="text-[#888888] text-[11px] uppercase tracking-wide">{t.recipient}: {PAYMENT_RECIPIENT}</p>
                  <p className="text-[#FCFCFC] text-sm font-bold truncate mt-0.5">{PAYMENT_CARD_NUMBER}</p>
                </div>
                <button
                  type="button"
                  onClick={handleCopyCard}
                  className="shrink-0 inline-flex items-center gap-1.5 bg-[#1a1a1a] border border-[#F08800]/55 rounded-lg px-2.5 py-2 text-[#FCFCFC] text-[11px] font-bold uppercase"
                >
                  <Copy className="w-3.5 h-3.5 text-[#F08800]" strokeWidth={2.2} />
                  {t.copy}
                </button>
              </div>

              <div className="mt-3 bg-[#1a1a1a] border border-[#F08800]/45 rounded-lg px-3 py-2.5 inline-flex items-center gap-2">
                <span className="text-[#f5c983] text-xs">{t.timer}:</span>
                <span className="text-[#FCFCFC] text-sm font-black font-mono tabular-nums min-w-[4.8ch] text-center">
                  {formatCountdown(secondsLeft)}
                </span>
              </div>
            </div>

            <label
              htmlFor="balance-topup-upload"
              className="block bg-[#1a1a1a] border border-dashed border-[#3a3a3a] hover:border-[#F08800]/60 rounded-lg p-4 cursor-pointer transition-all"
            >
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#F08800]/10 rounded-lg flex items-center justify-center">
                  <Upload className="w-5 h-5 text-[#F08800]" strokeWidth={2.2} />
                </div>
                <div className="min-w-0">
                  <p className="text-[#FCFCFC] text-sm font-bold truncate">
                    {screenshot ? screenshot.name : t.chooseFile}
                  </p>
                  <p className="text-[#888888] text-xs mt-1">{t.fileHint}</p>
                </div>
              </div>
              <input
                id="balance-topup-upload"
                type="file"
                accept="image/png,image/jpeg,image/jpg,image/webp"
                className="hidden"
                onChange={handleFileChange}
                disabled={isSubmitting || secondsLeft <= 0}
              />
            </label>
          </div>
        )}

        {copyHint && (
          <div className="mt-3 bg-[#0f2916] border border-[#22c55e]/45 rounded-lg p-2.5">
            <p className="text-[#86efac] text-xs">{copyHint}</p>
          </div>
        )}

        {isSessionLocked && (
          <div className="mt-3 bg-[#3b2004] border border-[#F08800]/45 rounded-lg p-2.5">
            <p className="text-[#f5c983] text-xs">{t.lockHint}</p>
          </div>
        )}

        {errorText && (
          <div className="mt-3 bg-[#7f1d1d]/50 border border-[#ef4444]/70 rounded-lg p-3">
            <p className="text-[#fecaca] text-xs leading-relaxed">{errorText}</p>
          </div>
        )}

        <div className="mt-4 flex gap-2.5">
          {step === 2 && (
            <button
              type="button"
              onClick={handleBackToIntro}
              className="flex-1 py-3.5 rounded-lg font-black uppercase tracking-wide text-xs bg-[#2a2a2a] text-[#FCFCFC]"
            >
              {t.back}
            </button>
          )}

          {step === 1 && (
            <button
              type="button"
              onClick={handleNextFromIntro}
              className="w-full py-3.5 rounded-lg font-black uppercase tracking-wide text-sm bg-[#F08800] text-[#121212] hover:bg-[#d97700]"
            >
              {t.next}
            </button>
          )}

          {step === 2 && (
            <button
              type="button"
              onClick={handleStartUploadStep}
              className="flex-[1.35] py-3.5 rounded-lg font-black uppercase tracking-wide text-[11px] bg-[#F08800] text-[#121212] hover:bg-[#d97700]"
            >
              {t.toUploadStep}
            </button>
          )}

          {step === 3 && secondsLeft <= 0 && (
            <button
              type="button"
              onClick={handleRestartSession}
              className="w-full py-3.5 rounded-lg font-black uppercase tracking-wide text-sm bg-[#F08800] text-[#121212] hover:bg-[#d97700]"
            >
              {t.restartSession}
            </button>
          )}

          {step === 3 && secondsLeft > 0 && (
            <button
              type="button"
              onClick={() => void handleSubmit()}
              disabled={isSubmitting || userId <= 0}
              className={`w-full py-3.5 rounded-lg font-black uppercase tracking-wide text-sm transition-all ${
                !isSubmitting && userId > 0
                  ? "bg-[#F08800] text-[#121212] hover:bg-[#d97700]"
                  : "bg-[#2a2a2a] text-[#555555] cursor-not-allowed"
              }`}
            >
              {isSubmitting ? (
                <span className="inline-flex items-center gap-2 justify-center">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  {t.wait}
                </span>
              ) : (
                t.submit
              )}
            </button>
          )}
        </div>

        <div className="mt-4 flex items-center justify-center">
          <WalletCards className="w-4 h-4 text-[#555555]" strokeWidth={2.1} />
        </div>
      </div>
    </div>
  );
}
