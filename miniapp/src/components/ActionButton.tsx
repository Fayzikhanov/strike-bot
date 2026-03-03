import { ReactNode } from "react";

interface ActionButtonProps {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: "primary" | "secondary";
  type?: "button" | "submit";
  fullWidth?: boolean;
}

export function ActionButton({
  children,
  onClick,
  disabled = false,
  variant = "primary",
  type = "button",
  fullWidth = true,
}: ActionButtonProps) {
  const baseClasses = `
    ${fullWidth ? "w-full" : ""}
    py-4 px-6
    rounded-lg
    font-black
    uppercase
    tracking-wide
    text-sm
    transition-all
    duration-200
    active:scale-95
    shadow-lg
    border-2
  `;

  const variantClasses = {
    primary: disabled
      ? "bg-[#2a2a2a] text-[#555555] border-[#2a2a2a] cursor-not-allowed"
      : "bg-[#F08800] hover:bg-[#d97700] text-[#121212] border-[#F08800] shadow-[#F08800]/30 hover:shadow-[#F08800]/50",
    secondary: disabled
      ? "bg-[#1a1a1a] text-[#555555] border-[#2a2a2a] cursor-not-allowed"
      : "bg-[#2a2a2a] hover:bg-[#3a3a3a] text-[#FCFCFC] border-[#2a2a2a] hover:border-[#F08800]/50",
  };

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${baseClasses} ${variantClasses[variant]}`}
    >
      {children}
    </button>
  );
}
