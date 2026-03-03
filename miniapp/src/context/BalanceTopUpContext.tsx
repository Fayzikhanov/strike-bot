import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";

type BalanceTopUpContextValue = {
  isTopUpOpen: boolean;
  openTopUp: () => void;
  closeTopUp: () => void;
};

const BalanceTopUpContext = createContext<BalanceTopUpContextValue | null>(null);

export function BalanceTopUpProvider({ children }: { children: ReactNode }) {
  const [isTopUpOpen, setIsTopUpOpen] = useState(false);

  const openTopUp = useCallback(() => {
    setIsTopUpOpen(true);
  }, []);

  const closeTopUp = useCallback(() => {
    setIsTopUpOpen(false);
  }, []);

  const value = useMemo<BalanceTopUpContextValue>(
    () => ({
      isTopUpOpen,
      openTopUp,
      closeTopUp,
    }),
    [closeTopUp, isTopUpOpen, openTopUp],
  );

  return <BalanceTopUpContext.Provider value={value}>{children}</BalanceTopUpContext.Provider>;
}

export function useBalanceTopUp() {
  const context = useContext(BalanceTopUpContext);
  if (!context) {
    throw new Error("useBalanceTopUp must be used within BalanceTopUpProvider");
  }
  return context;
}
