import { NavLink } from "react-router-dom";
import { Server, Award, ShoppingCart, UserCircle2 } from "lucide-react";
import { useLanguage } from "../i18n/LanguageContext";

export function Navigation() {
  const { language } = useLanguage();

  const navItems = [
    { to: "/", icon: Server, label: language === "uz" ? "SERVERLAR" : "СЕРВЕРЫ" },
    { to: "/privileges", icon: Award, label: language === "uz" ? "IMTIYOZLAR" : "ПРИВИЛЕГИИ" },
    { to: "/purchase", icon: ShoppingCart, label: language === "uz" ? "XARID" : "ПОКУПКА" },
    { to: "/profile", icon: UserCircle2, label: language === "uz" ? "PROFIL" : "ПРОФИЛЬ" },
  ];

  return (
    <nav className="fixed bottom-0 left-0 right-0 z-50 bg-[#1a1a1a] border-t-2 border-[#2a2a2a] shadow-2xl">
      <div
        className="max-w-[480px] mx-auto"
        style={{ paddingBottom: "max(env(safe-area-inset-bottom), 0px)" }}
      >
        <div className="grid grid-cols-4 gap-0">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `flex flex-col items-center gap-1 py-3 px-2 transition-all relative ${
                  isActive
                    ? "text-[#F08800] bg-[#F08800]/10"
                    : "text-[#888888] hover:text-[#FCFCFC] hover:bg-[#2a2a2a]/50"
                }`
              }
            >
              {({ isActive }) => (
                <>
                  {isActive && (
                    <div className="absolute top-0 left-0 right-0 h-1 bg-[#F08800] shadow-lg shadow-[#F08800]/50" />
                  )}
                  <item.icon className="w-6 h-6" strokeWidth={2.5} />
                  <span className="text-xs font-bold uppercase tracking-wide">{item.label}</span>
                </>
              )}
            </NavLink>
          ))}
        </div>
      </div>
    </nav>
  );
}
