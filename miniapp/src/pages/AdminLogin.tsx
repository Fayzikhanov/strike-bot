import { FormEvent, useEffect, useState } from "react";
import { Eye, EyeOff, Lock, ShieldCheck } from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  fetchAdminSummary,
  getResolvedApiBaseUrl,
  setPreferredApiBaseUrl,
} from "../api/strikeApi";
import { readSavedAdminKey, saveAdminKey } from "../lib/adminAuth";

type LocationState = {
  error?: string;
};

export function AdminLogin() {
  const navigate = useNavigate();
  const location = useLocation();
  const [adminKey, setAdminKey] = useState("");
  const [showKey, setShowKey] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [apiBaseInput, setApiBaseInput] = useState(() => getResolvedApiBaseUrl());
  const [apiBaseNotice, setApiBaseNotice] = useState("");

  useEffect(() => {
    const saved = readSavedAdminKey().trim();
    if (saved) {
      navigate("/admin/dashboard", { replace: true });
      return;
    }
    const state = location.state as LocationState | null;
    if (state?.error) {
      setErrorMessage(state.error);
    }
  }, [location.state, navigate]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const safeKey = adminKey.trim();
    if (!safeKey) {
      setErrorMessage("Введите код доступа.");
      return;
    }

    setSubmitting(true);
    setErrorMessage("");
    setApiBaseNotice("");

    const safeApiBase = apiBaseInput.trim();
    if (safeApiBase) {
      const resolvedApiBase = setPreferredApiBaseUrl(safeApiBase);
      if (resolvedApiBase) {
        setApiBaseInput(resolvedApiBase);
      }
    }

    try {
      await fetchAdminSummary(safeKey);
      saveAdminKey(safeKey);
      navigate("/admin/dashboard", { replace: true });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось авторизоваться";
      setErrorMessage(message || "Не удалось авторизоваться");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0a0a0a] px-4 py-10 text-white">
      <div className="mx-auto flex min-h-[80vh] w-full max-w-[420px] items-center justify-center">
        <div className="relative w-full overflow-hidden rounded-2xl border border-[#2a2a2a] bg-[#141414] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.45)]">
          <div className="pointer-events-none absolute -right-20 -top-20 h-44 w-44 rounded-full bg-[#f08800]/20 blur-3xl" />
          <div className="relative">
            <div className="mb-5 flex items-center gap-3">
              <div className="rounded-xl border border-[#f08800]/40 bg-[#f08800]/10 p-2.5">
                <ShieldCheck className="h-5 w-5 text-[#f08800]" />
              </div>
              <div>
                <p className="text-[11px] uppercase tracking-[0.18em] text-[#8b8b8b]">Strike.Uz Admin</p>
                <h1 className="mt-1 text-2xl font-black uppercase leading-none">Авторизация</h1>
              </div>
            </div>

            <p className="mb-6 text-sm text-[#a6a6ad]">
              Введите код доступа (`X-Admin-Key`), чтобы открыть админ-панель.
            </p>

            <form className="space-y-4" onSubmit={handleSubmit}>
              <label className="block text-xs uppercase tracking-[0.14em] text-[#7d7d85]">Код доступа</label>
              <div className="relative">
                <Lock className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#6f6f77]" />
                <input
                  type={showKey ? "text" : "password"}
                  value={adminKey}
                  onChange={(event) => setAdminKey(event.target.value)}
                  placeholder="Введите X-Admin-Key"
                  autoComplete="current-password"
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] py-3 pl-9 pr-10 text-sm text-white outline-none ring-0 focus:border-[#f08800]"
                />
                <button
                  type="button"
                  onClick={() => setShowKey((current) => !current)}
                  className="absolute right-2 top-1/2 inline-flex h-8 w-8 -translate-y-1/2 items-center justify-center rounded-lg text-[#8f8f98] hover:bg-[#1f1f24] hover:text-white"
                >
                  {showKey ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>

              <label className="block text-xs uppercase tracking-[0.14em] text-[#7d7d85]">API URL</label>
              <input
                type="text"
                value={apiBaseInput}
                onChange={(event) => {
                  setApiBaseInput(event.target.value);
                  setApiBaseNotice("");
                }}
                placeholder="https://...trycloudflare.com"
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-3 text-sm text-white outline-none ring-0 focus:border-[#f08800]"
              />
              <button
                type="button"
                onClick={() => {
                  const resolved = setPreferredApiBaseUrl(apiBaseInput.trim());
                  setApiBaseInput(resolved);
                  setApiBaseNotice(resolved ? "API URL применён." : "API URL сброшен.");
                }}
                className="w-full rounded-xl border border-[#3a3a45] bg-[#1a1a1f] px-4 py-2.5 text-xs font-bold uppercase tracking-[0.08em] text-[#d4d4dc]"
              >
                Применить API URL
              </button>

              {apiBaseNotice && (
                <div className="rounded-xl border border-[#2f5f2f] bg-[#0f2710] px-3 py-2 text-sm text-[#9ae6a0]">
                  {apiBaseNotice}
                </div>
              )}

              {errorMessage && (
                <div className="rounded-xl border border-[#7f1d1d] bg-[#3b1212] px-3 py-2 text-sm text-[#fca5a5]">
                  {errorMessage}
                </div>
              )}

              <button
                type="submit"
                disabled={submitting}
                className="w-full rounded-xl bg-[#f08800] px-4 py-3 text-sm font-black uppercase tracking-[0.08em] text-[#111111] disabled:opacity-60"
              >
                {submitting ? "Проверка..." : "Войти"}
              </button>
            </form>

            <div className="mt-5 text-xs text-[#767680]">
              API: {getResolvedApiBaseUrl() || "relative (/api)"}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
