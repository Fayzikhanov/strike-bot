import { useEffect, useState } from "react";
import logoSymbol from "../assets/logo-white.svg";

export function LoadingScreen() {
  const [isVisible, setIsVisible] = useState(true);

  useEffect(() => {
    const timer = setTimeout(() => {
      setIsVisible(false);
    }, 1500);

    return () => clearTimeout(timer);
  }, []);

  if (!isVisible) return null;

  return (
    <div className="fixed inset-0 z-[100] bg-[#121212] flex items-center justify-center">
      <div className="text-center">
        <div className="animate-pulse mb-6">
          <img src={logoSymbol} alt="Strike.Uz" className="h-16 w-auto mx-auto" />
        </div>
        <div className="flex gap-2 justify-center">
          <div
            className="w-2 h-2 bg-[#F08800] rounded-full animate-bounce"
            style={{ animationDelay: "0ms" }}
          />
          <div
            className="w-2 h-2 bg-[#F08800] rounded-full animate-bounce"
            style={{ animationDelay: "150ms" }}
          />
          <div
            className="w-2 h-2 bg-[#F08800] rounded-full animate-bounce"
            style={{ animationDelay: "300ms" }}
          />
        </div>
      </div>
    </div>
  );
}
