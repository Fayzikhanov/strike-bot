import logoHorizontal from "../assets/header-logo.svg";
import { useLanguage } from "../i18n/LanguageContext";

export function Header() {
  const { language, setLanguage } = useLanguage();

  const languageOptions = [
    { code: "uz" as const, flag: "🇺🇿", label: "O'zbek tili" },
    { code: "ru" as const, flag: "🇷🇺", label: "Русский язык" },
  ];

  return (
    <header
      className="sticky top-0 z-50 bg-[#121212] border-b border-[#2a2a2a] backdrop-blur-sm"
      style={{ paddingTop: "max(env(safe-area-inset-top), 0px)" }}
    >
      <div className="container mx-auto px-4 py-3 flex items-center justify-between gap-3">
        <img src={logoHorizontal} alt="Strike.Uz" className="h-8 w-auto flex-shrink-0" />
        <div className="flex items-center gap-2">
          {languageOptions.map((option) => {
            const isSelected = language === option.code;
            return (
              <button
                key={option.code}
                onClick={() => setLanguage(option.code)}
                aria-label={option.label}
                className={`w-9 h-9 rounded-lg border text-xl leading-none flex items-center justify-center transition-all ${
                  isSelected
                    ? "border-[#F08800] bg-[#F08800]/15 shadow-lg shadow-[#F08800]/20"
                    : "border-[#2a2a2a] bg-[#1a1a1a] opacity-80 hover:opacity-100"
                }`}
              >
                <span>{option.flag}</span>
              </button>
            );
          })}
        </div>
      </div>
    </header>
  );
}
