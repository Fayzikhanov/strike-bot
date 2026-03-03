import type { LiveServer } from "../api/strikeApi";

export const PUBLIC_SERVER_PORTS = new Set<number>([27015, 27016, 27017, 27018]);
const CSDM_SERVER_PORT = 27017;
const HIDE_N_SEEK_SERVER_PORT = 27018;
const BONUS_ENABLED_PUBLIC_PORTS = new Set<number>([27015, 27016]);

export const PURCHASABLE_PRIVILEGE_IDS = [
  "vip",
  "prime",
  "legend",
  "moder",
  "admin",
] as const;

export type PurchasablePrivilegeId = (typeof PURCHASABLE_PRIVILEGE_IDS)[number];

export interface TariffOption {
  months: 1 | 2 | 3;
  fullPrice: number;
  finalPrice: number;
}

type ServerOfferConfig = Partial<Record<PurchasablePrivilegeId, readonly TariffOption[]>>;

const VIP_PUBLIC_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 29000, finalPrice: 29000 },
  { months: 2, fullPrice: 58000, finalPrice: 50000 },
  { months: 3, fullPrice: 87000, finalPrice: 70000 },
];

const PRIME_PUBLIC_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 49000, finalPrice: 49000 },
  { months: 2, fullPrice: 98000, finalPrice: 90000 },
  { months: 3, fullPrice: 147000, finalPrice: 120000 },
];

const LEGEND_PUBLIC_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 79000, finalPrice: 79000 },
  { months: 2, fullPrice: 158000, finalPrice: 140000 },
  { months: 3, fullPrice: 237000, finalPrice: 180000 },
];

const VIP_DUST_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 25000, finalPrice: 25000 },
  { months: 2, fullPrice: 50000, finalPrice: 45000 },
  { months: 3, fullPrice: 75000, finalPrice: 60000 },
];

const MODER_CSDM_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 60000, finalPrice: 60000 },
  { months: 2, fullPrice: 120000, finalPrice: 105000 },
  { months: 3, fullPrice: 180000, finalPrice: 150000 },
];

const ADMIN_CSDM_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 100000, finalPrice: 100000 },
  { months: 2, fullPrice: 200000, finalPrice: 175000 },
  { months: 3, fullPrice: 300000, finalPrice: 240000 },
];

const VIP_HNS_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 15000, finalPrice: 15000 },
  { months: 2, fullPrice: 30000, finalPrice: 25000 },
  { months: 3, fullPrice: 45000, finalPrice: 38000 },
];

const MODER_MIX_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 80000, finalPrice: 80000 },
  { months: 2, fullPrice: 160000, finalPrice: 140000 },
  { months: 3, fullPrice: 240000, finalPrice: 190000 },
];

const ADMIN_MIX_TARIFFS: readonly TariffOption[] = [
  { months: 1, fullPrice: 100000, finalPrice: 100000 },
  { months: 2, fullPrice: 200000, finalPrice: 175000 },
  { months: 3, fullPrice: 300000, finalPrice: 240000 },
];

const PUBLIC_OFFERS_BY_PORT: Record<number, ServerOfferConfig> = {
  27015: {
    vip: VIP_PUBLIC_TARIFFS,
    prime: PRIME_PUBLIC_TARIFFS,
    legend: LEGEND_PUBLIC_TARIFFS,
  },
  27016: {
    vip: VIP_DUST_TARIFFS,
  },
  [CSDM_SERVER_PORT]: {
    moder: MODER_CSDM_TARIFFS,
    admin: ADMIN_CSDM_TARIFFS,
  },
  [HIDE_N_SEEK_SERVER_PORT]: {
    vip: VIP_HNS_TARIFFS,
    moder: MODER_CSDM_TARIFFS,
    admin: ADMIN_CSDM_TARIFFS,
  },
};

const MIX_SERVER_OFFERS: ServerOfferConfig = {
  moder: MODER_MIX_TARIFFS,
  admin: ADMIN_MIX_TARIFFS,
};

const EMPTY_SERVER_OFFERS: ServerOfferConfig = {};

function normalizeServerPort(server: LiveServer): number | null {
  const rawPort = (server as { port?: unknown }).port;
  const parsedPort =
    typeof rawPort === "number"
      ? rawPort
      : Number.parseInt(String(rawPort ?? "").trim(), 10);

  if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
    return null;
  }

  return parsedPort;
}

export function isPublicServer(server: LiveServer): boolean {
  const port = normalizeServerPort(server);
  return port !== null && PUBLIC_SERVER_PORTS.has(port);
}

export function serverSupportsBonus(server: LiveServer): boolean {
  const port = normalizeServerPort(server);
  return port !== null && BONUS_ENABLED_PUBLIC_PORTS.has(port);
}

export function isPurchasablePrivilegeId(
  privilegeId: string | null | undefined,
): privilegeId is PurchasablePrivilegeId {
  if (!privilegeId) {
    return false;
  }

  return (PURCHASABLE_PRIVILEGE_IDS as readonly string[]).includes(privilegeId);
}

export function getAllowedPrivilegeIdsForServer(
  server: LiveServer,
): readonly PurchasablePrivilegeId[] {
  return Object.keys(getServerOfferConfig(server)) as PurchasablePrivilegeId[];
}

export function getPrivilegeTariffsForServer(
  server: LiveServer,
  privilegeId: PurchasablePrivilegeId,
): readonly TariffOption[] {
  const offers = getServerOfferConfig(server);
  return offers[privilegeId] ?? [];
}

function getServerOfferConfig(server: LiveServer): ServerOfferConfig {
  const port = normalizeServerPort(server);
  if (port !== null) {
    const publicOffers = PUBLIC_OFFERS_BY_PORT[port];
    if (publicOffers) {
      return publicOffers;
    }
  }

  if (!isPublicServer(server)) {
    return MIX_SERVER_OFFERS;
  }

  return EMPTY_SERVER_OFFERS;
}

export function serverSupportsPrivilege(
  server: LiveServer,
  privilegeId: string,
): privilegeId is PurchasablePrivilegeId {
  return getAllowedPrivilegeIdsForServer(server).includes(privilegeId as PurchasablePrivilegeId);
}
