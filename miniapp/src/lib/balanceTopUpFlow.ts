export type TopUpFlowStep = "intro" | "card" | "upload";

export type TopUpUploadSession = {
  sessionId: string;
  startedAt: number;
  expiresAt: number;
};

export type PersistedTopUpFlow = {
  version: 1;
  userId: number;
  step: TopUpFlowStep;
  session: TopUpUploadSession | null;
  updatedAt: number;
};

const TOPUP_FLOW_STORAGE_KEY = "strike_balance_topup_flow_v1";

function normalizeSession(rawValue: unknown): TopUpUploadSession | null {
  if (!rawValue || typeof rawValue !== "object") {
    return null;
  }

  const source = rawValue as Partial<TopUpUploadSession>;
  const sessionId = String(source.sessionId ?? "").trim();
  const startedAt = Number(source.startedAt ?? 0);
  const expiresAt = Number(source.expiresAt ?? 0);

  if (!sessionId || !Number.isFinite(startedAt) || !Number.isFinite(expiresAt) || expiresAt <= startedAt) {
    return null;
  }

  return {
    sessionId,
    startedAt: Math.floor(startedAt),
    expiresAt: Math.floor(expiresAt),
  };
}

function normalizeStep(rawValue: unknown): TopUpFlowStep {
  const value = String(rawValue ?? "").trim().toLowerCase();
  if (value === "card" || value === "upload" || value === "intro") {
    return value;
  }
  return "intro";
}

export function readPersistedTopUpFlow(): PersistedTopUpFlow | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(TOPUP_FLOW_STORAGE_KEY);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw) as Partial<PersistedTopUpFlow>;
    const userId = Number(parsed.userId ?? 0);
    if (!Number.isFinite(userId) || userId <= 0) {
      return null;
    }

    const step = normalizeStep(parsed.step);
    const session = normalizeSession(parsed.session);
    const updatedAt = Number(parsed.updatedAt ?? 0);

    return {
      version: 1,
      userId: Math.floor(userId),
      step,
      session,
      updatedAt: Number.isFinite(updatedAt) ? Math.floor(updatedAt) : 0,
    };
  } catch {
    return null;
  }
}

export function readPersistedTopUpFlowForUser(userId: number): PersistedTopUpFlow | null {
  const safeUserId = Number(userId);
  if (!Number.isFinite(safeUserId) || safeUserId <= 0) {
    return null;
  }
  const flow = readPersistedTopUpFlow();
  if (!flow || flow.userId !== Math.floor(safeUserId)) {
    return null;
  }
  return flow;
}

export function savePersistedTopUpFlow(flow: PersistedTopUpFlow): void {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(TOPUP_FLOW_STORAGE_KEY, JSON.stringify(flow));
  } catch {
    // ignore storage write errors
  }
}

export function clearPersistedTopUpFlow(): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(TOPUP_FLOW_STORAGE_KEY);
  } catch {
    // ignore storage remove errors
  }
}

export function isTopUpUploadSessionActive(flow: PersistedTopUpFlow | null): boolean {
  if (!flow || flow.step !== "upload" || !flow.session) {
    return false;
  }
  const now = Math.floor(Date.now() / 1000);
  return now < flow.session.expiresAt;
}

export function hasActiveTopUpUploadSession(userId: number): boolean {
  return isTopUpUploadSessionActive(readPersistedTopUpFlowForUser(userId));
}
