const ADMIN_KEY_STORAGE_KEY = "strike_admin_api_key_v1";

export function readSavedAdminKey(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return window.localStorage.getItem(ADMIN_KEY_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

export function saveAdminKey(value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const safeValue = value.trim();
    if (safeValue) {
      window.localStorage.setItem(ADMIN_KEY_STORAGE_KEY, safeValue);
    } else {
      window.localStorage.removeItem(ADMIN_KEY_STORAGE_KEY);
    }
  } catch {
    // ignore storage errors
  }
}

export function clearSavedAdminKey(): void {
  saveAdminKey("");
}
