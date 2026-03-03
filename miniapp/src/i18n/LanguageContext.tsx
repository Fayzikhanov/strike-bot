import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from "react";

export type Language = "ru" | "uz";

interface LanguageContextValue {
  language: Language;
  setLanguage: (language: Language) => void;
}

const STORAGE_KEY = "strikeuz-miniapp-language";

const LanguageContext = createContext<LanguageContextValue | null>(null);

function readStoredLanguage(): Language {
  if (typeof window === "undefined") {
    return "ru";
  }

  const storedValue = window.localStorage.getItem(STORAGE_KEY);
  return storedValue === "uz" ? "uz" : "ru";
}

export function LanguageProvider({ children }: PropsWithChildren) {
  const [language, setLanguage] = useState<Language>(readStoredLanguage);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(STORAGE_KEY, language);
  }, [language]);

  const contextValue = useMemo(
    () => ({
      language,
      setLanguage,
    }),
    [language],
  );

  return <LanguageContext.Provider value={contextValue}>{children}</LanguageContext.Provider>;
}

export function useLanguage(): LanguageContextValue {
  const context = useContext(LanguageContext);
  if (!context) {
    throw new Error("useLanguage must be used inside LanguageProvider");
  }
  return context;
}
