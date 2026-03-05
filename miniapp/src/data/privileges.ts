import vipIcon from "../assets/vip-icon.png";
import primeIcon from "../assets/prime-icon.png";
import legendIcon from "../assets/legend-icon.png";
import moderIcon from "../assets/moder-icon.png";
import adminIcon from "../assets/admin-icon.png";
import glAdminIcon from "../assets/gl-admin-icon.png";
import adminCwIcon from "../assets/admin-cw-icon.png";

export interface Privilege {
  id: string;
  name: string;
  color: string;
  features: string[];
  price: string;
  icon: string;
  iconImage?: string;
}

export const privileges: Privilege[] = [
  {
    id: "vip",
    name: "VIP",
    color: "#22c55e",
    price: "от 15 000 UZS / месяц",
    icon: "⭐",
    iconImage: vipIcon,
    features: [
      "1 скин оружия (автоматически)",
      "1 скин персонажа (без выбора, автоматически)",
      "1 скин ножа (без выбора, автоматически)",
      "Просмотр нанесённого урона",
      "+10 HP за убийство",
      "Пакет гранат (без smoke и molotov)",
      "Шприц: +15 HP",
      "Стартовый HP: 110",
      "Максимальный HP: 110",
    ],
  },
  {
    id: "prime",
    name: "PRIME",
    color: "#3b82f6",
    price: "49 000 UZS / месяц",
    icon: "💎",
    iconImage: primeIcon,
    features: [
      "1 скин оружия (лучше, чем у VIP)",
      "1 скин персонажа (без выбора, автоматически, лучше VIP)",
      "1 скин ножа (без выбора, автоматически, лучше VIP)",
      "Просмотр нанесённого урона",
      "+20 HP за убийство",
      "Пакет гранат + molotov",
      "Шприц: +50 HP",
      "HP: 120",
      "Максимальный HP: 120",
    ],
  },
  {
    id: "legend",
    name: "LEGEND",
    color: "#F08800",
    price: "79 000 UZS / месяц",
    icon: "👑",
    iconImage: legendIcon,
    features: [
      "1 скин оружия (лучше, чем у PRIME)",
      "1 скин персонажа (без выбора, автоматически, лучше PRIME)",
      "3 скина ножа (с выбором, лучше PRIME)",
      "Просмотр нанесённого урона",
      "+30 HP за убийство",
      "Пакет гранат + molotov + smoke",
      "Шприц: +100 HP",
      "HP: 130",
      "Максимальный HP: 130",
      "AUTOBHOP",
    ],
  },
  {
    id: "moder",
    name: "MODER",
    color: "#8b5cf6",
    price: "от 60 000 UZS / месяц",
    icon: "🛡️",
    iconImage: moderIcon,
    features: [
      "Права Kick/Ban",
      "Mute/Gag игроков",
      "Управление голосованием за карту",
      "Инструменты модератора сервера",
      "Значок модератора",
    ],
  },
  {
    id: "admin",
    name: "ADMIN",
    color: "#ef4444",
    price: "100 000 UZS / месяц",
    icon: "⚔️",
    iconImage: adminIcon,
    features: [
      "Все возможности MODER",
      "Полные права администратора",
      "Расширенная система банов",
      "Резервный слот",
    ],
  },
  {
    id: "gl-admin",
    name: "GL ADMIN",
    color: "#dc2626",
    price: "По согласованию",
    icon: "🔥",
    iconImage: glAdminIcon,
    features: [
      "Все возможности ADMIN",
      "Глобальные права на сервере",
      "Расширенные права старшего админа",
      "Управление командой админов",
      "Модерация сообщества",
      "Специальный GL-значок",
    ],
  },
  {
    id: "admin-cw",
    name: "ADMIN CW/MIX",
    color: "#06b6d4",
    price: "По заявке",
    icon: "🎮",
    iconImage: adminCwIcon,
    features: [
      "Управление CW/MIX серверами",
      "Инструменты настройки матчей",
      "Управление командами",
      "CWAR меню",
      "Проведение турниров",
    ],
  },
];

export const getPrivilegeById = (id: string): Privilege | undefined => {
  return privileges.find((priv) => priv.id === id);
};
