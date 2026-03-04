import { useEffect, useMemo } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { Header } from "./Header";
import { Navigation } from "./Navigation";
import { LoadingScreen } from "./LoadingScreen";
import { initTelegramWebApp } from "../lib/initTelegramWebApp";
import { useLanguage } from "../i18n/LanguageContext";
import { BalanceTopUpModal } from "./BalanceTopUpModal";
import { useBalanceTopUp } from "../context/BalanceTopUpContext";
import { hasActiveTopUpUploadSession } from "../lib/balanceTopUpFlow";
import type { BalanceTopUpResponse } from "../api/strikeApi";
import { sendActivityPing } from "../api/strikeApi";

export function Layout() {
  const location = useLocation();
  const { language } = useLanguage();
  const { isTopUpOpen, openTopUp, closeTopUp } = useBalanceTopUp();

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

  useEffect(() => {
    initTelegramWebApp();
  }, []);

  useEffect(() => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
  }, [location.pathname]);

  useEffect(() => {
    if (telegramUserId <= 0) {
      return;
    }
    if (hasActiveTopUpUploadSession(telegramUserId)) {
      openTopUp();
    }
  }, [openTopUp, telegramUserId]);

  useEffect(() => {
    if (telegramUserId <= 0 || typeof window === "undefined") {
      return;
    }

    const tgUser = (
      window as Window & {
        Telegram?: {
          WebApp?: {
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
    ).Telegram?.WebApp?.initDataUnsafe?.user;

    sendActivityPing({
      userId: telegramUserId,
      username: tgUser?.username ?? "",
      firstName: tgUser?.first_name ?? "",
      lastName: tgUser?.last_name ?? "",
      source: "miniapp_open",
      language,
    }).catch(() => {
      // ignore activity ping errors
    });
  }, [language, telegramUserId]);

  const handleTopUpSuccess = (response: BalanceTopUpResponse) => {
    if (typeof window === "undefined") {
      return;
    }
    window.dispatchEvent(new CustomEvent("strike:balance-topup-success", { detail: response }));
  };

  return (
    <>
      <LoadingScreen />
      <div className="min-h-screen w-full bg-background flex flex-col max-w-[480px] mx-auto">
        <Header />
        <main
          className="flex-1"
          style={{ paddingBottom: 'calc(4.5rem + env(safe-area-inset-bottom))' }}
        >
          <Outlet />
        </main>
        <Navigation />
      </div>
      <BalanceTopUpModal
        isOpen={isTopUpOpen}
        onClose={closeTopUp}
        userId={telegramUserId}
        language={language}
        onSuccess={handleTopUpSuccess}
      />
    </>
  );
}
