export type ServerStatus = "online" | "offline";

export interface LiveServer {
  id: string;
  port: number;
  name: string;
  map: string;
  players: number;
  max: number;
  maxPlayers: number;
  ip: string;
  status: ServerStatus;
}

export interface LivePlayer {
  id: string;
  nickname: string;
  kills: number;
  deaths: number | null;
  time: number;
}

export interface PurchaseConfirmedPayload {
  userId: number;
  useBalance?: boolean;
  productType?: "privilege" | "bonus";
  identifierType?: "nickname" | "steam";
  serverId: string;
  server: string;
  amount: number;
  privilege?: string;
  duration?: string;
  durationMonths?: number;
  nickname?: string;
  password?: string;
  currentPassword?: string;
  renewalRequested?: boolean;
  changePassword?: boolean;
  steamId?: string;
  bonusAmount?: number;
  bonusPackageLabel?: string;
  bonusNickname?: string;
  bonusBefore?: number;
  username?: string;
  firstName?: string;
  lastName?: string;
  screenshotDataUrl?: string;
  screenshotName?: string;
  screenshotMimeType?: string;
  paymentSessionId?: string;
  paymentSessionStartedAt?: number;
  paymentSessionExpiresAt?: number;
  language: "ru" | "uz";
}

export interface UserBalanceResponse {
  ok: boolean;
  balance: number;
  updatedAt: number;
  timestamp: number;
}

export interface BalanceHistoryItem {
  id: string;
  createdAt: number;
  type: string;
  delta: number;
  before: number;
  after: number;
  meta: Record<string, unknown>;
}

export interface BalanceHistoryResponse {
  ok: boolean;
  items: BalanceHistoryItem[];
  total: number;
  timestamp: number;
}

export interface UserPrivilegeItem {
  id: string;
  createdAt: number;
  serverId: string;
  serverName: string;
  privilegeKey: string;
  privilegeLabel: string;
  identifierType: "nickname" | "steam";
  nickname: string;
  steamId: string;
  remainingDays: number;
  totalDays: number;
  daysPassed: number;
  canRenew: boolean;
}

export interface UserPrivilegesResponse {
  ok: boolean;
  items: UserPrivilegeItem[];
  total: number;
  timestamp: number;
}

export interface BalanceTopUpPayload {
  userId: number;
  username?: string;
  firstName?: string;
  lastName?: string;
  screenshotDataUrl: string;
  screenshotName: string;
  screenshotMimeType: string;
  topupSessionId: string;
  topupSessionStartedAt: number;
  topupSessionExpiresAt: number;
  language: "ru" | "uz";
}

export interface BalanceTopUpResponse {
  ok: boolean;
  timestamp: number;
  creditedAmount: number;
  balanceBefore: number;
  balanceAfter: number;
  paymentVerification: {
    ok: boolean;
    mode: string;
  };
  balance: {
    balance: number;
    updatedAt: number;
  };
}

export interface BonusAccountInfo {
  steamId: string;
  nickname: string;
  bonusCount: number;
  database: string;
}

export interface PrivilegeAccountInfo {
  supported: boolean;
  exists: boolean;
  identifierType: "nickname" | "steam";
  nickname: string;
  steamId: string;
  password?: string;
  privilege: string;
  flags: string;
  days: number;
  isPermanent: boolean;
  isDisabled: boolean;
  isExpired: boolean;
}

export interface PurchaseConfirmedResponse {
  ok: boolean;
  timestamp: number;
  purchaseId: string;
  reportSent: boolean;
  productType: "privilege" | "bonus";
  balance?: {
    balance: number;
    spent: number;
    before: number;
    after: number;
    source: "balance" | "card";
  };
  cashback?: {
    amount: number;
    percent: number;
    before: number;
    after: number;
  } | null;
  bonusResult?: {
    steamId: string;
    nickname: string;
    added: number;
    before: number;
    after: number;
    database: string;
  };
}

interface ServersResponse {
  servers: LiveServer[];
  total: number;
  timestamp: number;
}

interface ServerPlayersResponse {
  server: LiveServer;
  players: LivePlayer[];
  timestamp: number;
}

interface BonusAccountResponse {
  ok: boolean;
  account: BonusAccountInfo;
  timestamp: number;
}

interface PrivilegeAccountResponse {
  ok: boolean;
  account: PrivilegeAccountInfo;
  timestamp: number;
}

interface PrivilegePasswordVerifyResponse {
  ok: boolean;
  valid: boolean;
  account: PrivilegeAccountInfo;
  timestamp: number;
}

export interface PaymentStatusInfo {
  banned: boolean;
  blocked_until: number;
  seconds_remaining: number;
  reason: string;
  failures: number;
  max_attempts: number;
  ban_seconds: number;
}

interface PaymentStatusResponse {
  ok: boolean;
  status: PaymentStatusInfo;
  uploadSessionSeconds: number;
  timestamp: number;
}

interface FetchPrivilegeAccountParams {
  serverId: string;
  identifierType?: "nickname" | "steam";
  nickname?: string;
  steamId?: string;
  serverName?: string;
}

const DEFAULT_PRODUCTION_API_BASE_URL = "https://strikeuzbotapi.loca.lt";

function readRuntimeApiBaseUrl(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    const params = new URLSearchParams(window.location.search);
    return (params.get("api") ?? "").trim();
  } catch {
    return "";
  }
}

const RUNTIME_API_BASE_URL = readRuntimeApiBaseUrl();
const RAW_API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").trim();
const API_BASE_URL = (
  RUNTIME_API_BASE_URL ||
  RAW_API_BASE_URL ||
  (import.meta.env.PROD ? DEFAULT_PRODUCTION_API_BASE_URL : "")
).replace(/\/$/, "");
const SERVERS_CACHE_KEY = "strikeuz_live_servers_cache_v1";
const SERVERS_FETCH_ATTEMPTS = 3;
const PRIVILEGE_LOOKUP_FETCH_ATTEMPTS = 3;

function buildApiUrl(path: string): string {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

function buildApiHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: "application/json",
  };

  // localtunnel shows a browser warning page unless this header is present.
  if (API_BASE_URL.includes(".loca.lt")) {
    headers["bypass-tunnel-reminder"] = "true";
  }

  return headers;
}

async function buildApiError(response: Response): Promise<Error> {
  let apiMessage = "";
  try {
    const payload = (await response.json()) as { error?: unknown };
    if (typeof payload?.error === "string") {
      apiMessage = payload.error.trim();
    }
  } catch {
    // ignore JSON parse errors
  }

  if (apiMessage) {
    return new Error(apiMessage);
  }
  return new Error(`API request failed: ${response.status}`);
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(buildApiUrl(path), {
    headers: buildApiHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw await buildApiError(response);
  }

  return (await response.json()) as T;
}

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetch(buildApiUrl(path), {
    method: "POST",
    headers: {
      ...buildApiHeaders(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    cache: "no-store",
  });

  if (!response.ok) {
    throw await buildApiError(response);
  }

  return (await response.json()) as T;
}

async function getJsonWithRetry<T>(path: string, attempts: number): Promise<T> {
  let lastError: unknown = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await getJson<T>(path);
    } catch (error) {
      lastError = error;

      if (attempt < attempts) {
        const backoffMs = 350 * attempt;
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }
  }

  throw lastError ?? new Error("Failed to fetch JSON");
}

function readCachedServers(): LiveServer[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(SERVERS_CACHE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as { servers?: LiveServer[] };
    return Array.isArray(parsed.servers) ? parsed.servers : [];
  } catch {
    return [];
  }
}

function writeCachedServers(servers: LiveServer[]): void {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(
      SERVERS_CACHE_KEY,
      JSON.stringify({ servers, timestamp: Date.now() }),
    );
  } catch {
    // ignore localStorage write errors
  }
}

// Important: Servers/players in miniapp must come only from live Strike.Uz API (a2s).
export async function fetchServers(): Promise<LiveServer[]> {
  try {
    const payload = await getJsonWithRetry<ServersResponse>(
      "/api/servers",
      SERVERS_FETCH_ATTEMPTS,
    );
    writeCachedServers(payload.servers);
    return payload.servers;
  } catch (error) {
    const cachedServers = readCachedServers();
    if (cachedServers.length > 0) {
      return cachedServers;
    }

    throw error;
  }
}

export async function fetchServerPlayers(serverId: string): Promise<ServerPlayersResponse> {
  return getJson<ServerPlayersResponse>(`/api/servers/${serverId}/players`);
}

export async function fetchBonusAccount(
  serverId: string,
  steamId: string,
): Promise<BonusAccountInfo> {
  const params = new URLSearchParams({
    serverId,
    steamId,
  });
  const response = await getJson<BonusAccountResponse>(
    `/api/bonus-account?${params.toString()}`,
  );
  return response.account;
}

export async function fetchPrivilegeAccount({
  serverId,
  identifierType = "nickname",
  nickname = "",
  steamId = "",
  serverName = "",
}: FetchPrivilegeAccountParams): Promise<PrivilegeAccountInfo> {
  const params = new URLSearchParams({ serverId, identifierType });
  if (identifierType === "steam") {
    params.set("steamId", steamId);
  } else {
    params.set("nickname", nickname);
  }
  if (serverName.trim()) {
    params.set("serverName", serverName.trim());
  }
  const response = await getJsonWithRetry<PrivilegeAccountResponse>(
    `/api/privilege-account?${params.toString()}`,
    PRIVILEGE_LOOKUP_FETCH_ATTEMPTS,
  );
  return response.account;
}

export async function verifyPrivilegePassword(
  serverId: string,
  nickname: string,
  password: string,
  serverName = "",
): Promise<PrivilegePasswordVerifyResponse> {
  return postJson<PrivilegePasswordVerifyResponse>("/api/privilege-password-verify", {
    serverId,
    identifierType: "nickname",
    nickname,
    password,
    serverName,
  });
}

export async function notifyPurchaseConfirmed(
  payload: PurchaseConfirmedPayload,
): Promise<PurchaseConfirmedResponse> {
  return postJson<PurchaseConfirmedResponse>("/api/purchase-confirmed", payload);
}

export async function fetchUserBalance(userId: number): Promise<UserBalanceResponse> {
  const params = new URLSearchParams({ userId: String(userId) });
  return getJson<UserBalanceResponse>(`/api/balance?${params.toString()}`);
}

export async function fetchUserBalanceHistory(
  userId: number,
  limit = 120,
): Promise<BalanceHistoryResponse> {
  const params = new URLSearchParams({
    userId: String(userId),
    limit: String(Math.max(1, Math.floor(limit || 120))),
  });
  return getJson<BalanceHistoryResponse>(`/api/balance-history?${params.toString()}`);
}

export async function fetchUserPrivileges(
  userId: number,
  limit = 30,
): Promise<UserPrivilegesResponse> {
  const params = new URLSearchParams({
    userId: String(userId),
    limit: String(Math.max(1, Math.floor(limit || 30))),
  });
  return getJson<UserPrivilegesResponse>(`/api/user-privileges?${params.toString()}`);
}

export async function submitBalanceTopUp(
  payload: BalanceTopUpPayload,
): Promise<BalanceTopUpResponse> {
  return postJson<BalanceTopUpResponse>("/api/balance-topup", payload);
}

export async function fetchPaymentStatus(
  userId: number,
  paymentSessionId = "",
): Promise<PaymentStatusResponse> {
  const params = new URLSearchParams({ userId: String(userId) });
  const safeSessionId = paymentSessionId.trim();
  if (safeSessionId) {
    params.set("paymentSessionId", safeSessionId);
  }
  return getJson<PaymentStatusResponse>(`/api/payment-status?${params.toString()}`);
}
