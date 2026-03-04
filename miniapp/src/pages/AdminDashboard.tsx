import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Award,
  Calendar,
  CreditCard,
  DollarSign,
  Gift,
  LogOut,
  RefreshCw,
  Search,
  TrendingUp,
  Users,
  Wallet,
  Zap,
  type LucideIcon,
} from "lucide-react";
import { Navigate, useNavigate } from "react-router-dom";
import {
  fetchAdminSummary,
  fetchAdminUsers,
  getResolvedApiBaseUrl,
  submitAdminBalanceAdjust,
  type AdminSummaryData,
  type AdminUserItem,
} from "../api/strikeApi";
import { clearSavedAdminKey, readSavedAdminKey } from "../lib/adminAuth";

const USERS_PER_PAGE = 30;

type BalanceAdjustModalState = {
  user: AdminUserItem;
  amount: string;
  comment: string;
  adminLabel: string;
  submitting: boolean;
  error: string;
};

type SummaryCardVisual = {
  icon: LucideIcon;
  iconClassName: string;
  iconSurfaceClassName: string;
  glowClassName: string;
};

const SUMMARY_CARD_VISUALS: SummaryCardVisual[] = [
  {
    icon: Users,
    iconClassName: "text-[#60A5FA]",
    iconSurfaceClassName: "bg-[#60A5FA]/12 border-[#60A5FA]/35",
    glowClassName: "from-[#60A5FA]/18",
  },
  {
    icon: DollarSign,
    iconClassName: "text-[#22C55E]",
    iconSurfaceClassName: "bg-[#22C55E]/12 border-[#22C55E]/35",
    glowClassName: "from-[#22C55E]/15",
  },
  {
    icon: Calendar,
    iconClassName: "text-[#C084FC]",
    iconSurfaceClassName: "bg-[#C084FC]/12 border-[#C084FC]/35",
    glowClassName: "from-[#C084FC]/15",
  },
  {
    icon: TrendingUp,
    iconClassName: "text-[#FB923C]",
    iconSurfaceClassName: "bg-[#FB923C]/12 border-[#FB923C]/35",
    glowClassName: "from-[#FB923C]/15",
  },
  {
    icon: Gift,
    iconClassName: "text-[#F472B6]",
    iconSurfaceClassName: "bg-[#F472B6]/12 border-[#F472B6]/35",
    glowClassName: "from-[#F472B6]/15",
  },
  {
    icon: Wallet,
    iconClassName: "text-[#34D399]",
    iconSurfaceClassName: "bg-[#34D399]/12 border-[#34D399]/35",
    glowClassName: "from-[#34D399]/15",
  },
  {
    icon: Zap,
    iconClassName: "text-[#FACC15]",
    iconSurfaceClassName: "bg-[#FACC15]/12 border-[#FACC15]/35",
    glowClassName: "from-[#FACC15]/15",
  },
  {
    icon: Award,
    iconClassName: "text-[#F87171]",
    iconSurfaceClassName: "bg-[#F87171]/12 border-[#F87171]/35",
    glowClassName: "from-[#F87171]/15",
  },
];

function formatMoney(value: number): string {
  const safe = Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
  return safe.toLocaleString("ru-RU");
}

function formatSignedMoney(value: number): string {
  const safe = Number.isFinite(value) ? Math.floor(value) : 0;
  const prefix = safe >= 0 ? "+" : "-";
  return `${prefix}${formatMoney(Math.abs(safe))}`;
}

function formatDateTime(timestamp: number): string {
  const safe = Number.isFinite(timestamp) ? Math.max(0, Math.floor(timestamp)) : 0;
  if (safe <= 0) {
    return "—";
  }
  const date = new Date(safe * 1000);
  return date.toLocaleString("ru-RU", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatRelative(timestamp: number): string {
  const safe = Number.isFinite(timestamp) ? Math.max(0, Math.floor(timestamp)) : 0;
  if (safe <= 0) {
    return "нет данных";
  }
  const diffSeconds = Math.max(Math.floor(Date.now() / 1000) - safe, 0);
  if (diffSeconds < 60) {
    return "только что";
  }
  if (diffSeconds < 3600) {
    return `${Math.floor(diffSeconds / 60)} мин назад`;
  }
  if (diffSeconds < 86400) {
    return `${Math.floor(diffSeconds / 3600)} ч назад`;
  }
  return `${Math.floor(diffSeconds / 86400)} дн назад`;
}

function formatPercent(value: number): string {
  const safe = Number.isFinite(value) ? Math.max(0, value) : 0;
  return `${safe.toFixed(2).replace(/\.00$/, "")}%`;
}

function getUserInitial(displayName: string): string {
  const first = displayName.trim().charAt(0).toUpperCase();
  return first || "U";
}

function isUnauthorizedError(message: string): boolean {
  const safe = message.toLowerCase();
  return (
    safe.includes("401") ||
    safe.includes("403") ||
    safe.includes("unauthorized") ||
    safe.includes("forbidden") ||
    safe.includes("x-admin-key") ||
    safe.includes("admin key")
  );
}

export function AdminDashboard() {
  const navigate = useNavigate();

  const [adminKey, setAdminKey] = useState<string>(() => readSavedAdminKey().trim());

  const [summary, setSummary] = useState<AdminSummaryData | null>(null);
  const [users, setUsers] = useState<AdminUserItem[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [successMessage, setSuccessMessage] = useState("");

  const [searchDraft, setSearchDraft] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalItems, setTotalItems] = useState(0);
  const [generatedAt, setGeneratedAt] = useState(0);
  const [expandedPrivilegesByUser, setExpandedPrivilegesByUser] = useState<Record<number, boolean>>({});
  const [expandedImportsByUser, setExpandedImportsByUser] = useState<Record<number, boolean>>({});
  const [expandedPurchasesByUser, setExpandedPurchasesByUser] = useState<Record<number, boolean>>({});
  const [expandedUsersByUser, setExpandedUsersByUser] = useState<Record<number, boolean>>({});

  const [adjustModal, setAdjustModal] = useState<BalanceAdjustModalState | null>(null);

  const isAuthorized = adminKey.trim().length > 0;

  const redirectToLogin = useCallback(
    (message?: string) => {
      clearSavedAdminKey();
      setAdminKey("");
      navigate("/admin/login", {
        replace: true,
        state: message ? { error: message } : undefined,
      });
    },
    [navigate],
  );

  const loadDashboard = useCallback(async () => {
    if (!adminKey) {
      return;
    }

    setIsLoading(true);
    setErrorMessage("");

    try {
      const [summaryResponse, usersResponse] = await Promise.all([
        fetchAdminSummary(adminKey),
        fetchAdminUsers(adminKey, {
          page,
          pageSize: USERS_PER_PAGE,
          search: searchQuery,
        }),
      ]);

      setSummary(summaryResponse.summary);
      setUsers(usersResponse.items);
      setTotalPages(Math.max(usersResponse.totalPages, 1));
      setTotalItems(usersResponse.totalItems);
      setGeneratedAt(usersResponse.generatedAt || summaryResponse.generatedAt || 0);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось загрузить админ-панель";
      if (isUnauthorizedError(message || "")) {
        redirectToLogin("Код доступа больше не действует. Введите его заново.");
        return;
      }
      setErrorMessage(message || "Не удалось загрузить админ-панель");
    } finally {
      setIsLoading(false);
    }
  }, [adminKey, page, redirectToLogin, searchQuery]);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    if (!adminKey) {
      return;
    }
    const interval = window.setInterval(() => {
      void loadDashboard();
    }, 30000);

    return () => {
      window.clearInterval(interval);
    };
  }, [adminKey, loadDashboard]);

  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages);
    }
  }, [page, totalPages]);

  const handleSearchSubmit = () => {
    setPage(1);
    setSearchQuery(searchDraft.trim());
  };

  const handleLogout = () => {
    redirectToLogin();
  };

  const openAdjustModal = (user: AdminUserItem) => {
    setAdjustModal({
      user,
      amount: "",
      comment: "",
      adminLabel: "dashboard-admin",
      submitting: false,
      error: "",
    });
  };

  const closeAdjustModal = () => {
    setAdjustModal(null);
  };

  const toggleUserPrivileges = (userId: number) => {
    setExpandedPrivilegesByUser((current) => ({
      ...current,
      [userId]: !current[userId],
    }));
  };

  const toggleUserPurchases = (userId: number) => {
    setExpandedPurchasesByUser((current) => ({
      ...current,
      [userId]: !current[userId],
    }));
  };

  const toggleUserImports = (userId: number) => {
    setExpandedImportsByUser((current) => ({
      ...current,
      [userId]: !current[userId],
    }));
  };

  const toggleUserExpanded = (userId: number) => {
    setExpandedUsersByUser((current) => ({
      ...current,
      [userId]: !current[userId],
    }));
  };

  const submitAdjustBalance = useCallback(async () => {
    if (!adjustModal || !adminKey) {
      return;
    }

    const parsedAmount = Number(adjustModal.amount.replace(/\s+/g, ""));
    const safeAmount = Number.isFinite(parsedAmount) ? Math.trunc(parsedAmount) : 0;
    const safeComment = adjustModal.comment.trim();

    if (safeAmount === 0) {
      setAdjustModal({ ...adjustModal, error: "Введите сумму (можно с минусом)." });
      return;
    }
    if (safeComment.length < 3) {
      setAdjustModal({ ...adjustModal, error: "Комментарий минимум 3 символа." });
      return;
    }

    setAdjustModal({ ...adjustModal, submitting: true, error: "" });

    try {
      const response = await submitAdminBalanceAdjust(adminKey, {
        userId: adjustModal.user.userId,
        amount: safeAmount,
        comment: safeComment,
        adminLabel: adjustModal.adminLabel.trim() || "dashboard-admin",
        username: adjustModal.user.username,
        firstName: adjustModal.user.firstName,
        lastName: adjustModal.user.lastName,
      });

      setSuccessMessage(
        `Баланс обновлён: ${formatSignedMoney(response.amount)} UZS, стало ${formatMoney(response.balanceAfter)} UZS`,
      );
      closeAdjustModal();
      await loadDashboard();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось изменить баланс";
      setAdjustModal((current) => (
        current ? { ...current, submitting: false, error: message || "Не удалось изменить баланс" } : current
      ));
    }
  }, [adjustModal, adminKey, loadDashboard]);

  const summaryCards = useMemo(() => {
    if (!summary) {
      return [] as Array<{ label: string; value: string; hint: string; visual: SummaryCardVisual }>;
    }

    const onboarding = summary.onboarding ?? {
      startedUsers: 0,
      welcomeBonusClaimedUsers: 0,
      welcomeBonusClaimRate: 0,
      welcomeBonusIssuedAmount: 0,
    };

    const base = [
      {
        label: "Активные за 24ч",
        value: String(summary.activeUsers24h),
        hint: `Всего пользователей: ${summary.totalUsers}`,
      },
      {
        label: "Платежи за день",
        value: `${summary.payments.day.count}`,
        hint: `${formatMoney(summary.payments.day.amount)} UZS`,
      },
      {
        label: "Платежи за неделю",
        value: `${summary.payments.week.count}`,
        hint: `${formatMoney(summary.payments.week.amount)} UZS`,
      },
      {
        label: "Платежи за месяц",
        value: `${summary.payments.month.count}`,
        hint: `${formatMoney(summary.payments.month.amount)} UZS`,
      },
      {
        label: "Куплено привилегий",
        value: `${summary.privileges.total}`,
        hint: `За месяц: ${summary.privileges.month}`,
      },
      {
        label: "Общий баланс",
        value: `${formatMoney(summary.totalBalance)} UZS`,
        hint: `Пользователей с балансом: ${summary.usersWithBalance}`,
      },
      {
        label: "Стартанули бота",
        value: `${onboarding.startedUsers}`,
        hint: `Получили бонус: ${onboarding.welcomeBonusClaimedUsers}`,
      },
      {
        label: "Welcome-бонус",
        value: formatPercent(onboarding.welcomeBonusClaimRate),
        hint: `Выдано: ${formatMoney(onboarding.welcomeBonusIssuedAmount)} UZS`,
      },
    ];

    return base.map((card, index) => ({
      ...card,
      visual: SUMMARY_CARD_VISUALS[index % SUMMARY_CARD_VISUALS.length],
    }));
  }, [summary]);

  if (!isAuthorized) {
    return <Navigate to="/admin/login" replace />;
  }

  return (
    <div className="min-h-screen bg-[#0a0a0a] text-white">
      <div className="mx-auto max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">
        <div className="space-y-6">
          <section className="relative overflow-hidden rounded-2xl border border-[#252529] bg-[#151518] p-5 sm:p-6">
            <div className="pointer-events-none absolute -right-24 -top-24 h-52 w-52 rounded-full bg-[#f08800]/16 blur-3xl" />
            <div className="relative flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
              <div>
                <p className="text-xs uppercase tracking-[0.18em] text-[#8a8a90]">Strike.Uz Admin</p>
                <h1 className="mt-2 text-3xl font-black uppercase leading-none sm:text-4xl">Дашборд бота</h1>
                <p className="mt-3 text-sm text-[#a8a8b0]">
                  Метрики, пользователи, LTV и ручные корректировки баланса.
                </p>
              </div>

              <div className="space-y-3">
                <div className="rounded-xl border border-[#2f2f35] bg-[#101012] px-3 py-2 text-xs text-[#8a8a90]">
                  <div>Обновлено: {generatedAt > 0 ? formatDateTime(generatedAt) : "—"}</div>
                  <div className="mt-1 break-all">API: {getResolvedApiBaseUrl() || "relative (/api)"}</div>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      void loadDashboard();
                    }}
                    disabled={isLoading}
                    className="inline-flex items-center gap-2 rounded-xl border border-[#32323a] bg-[#1f1f24] px-4 py-2 text-sm font-semibold text-[#d6d6dd] disabled:opacity-55"
                  >
                    <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
                    {isLoading ? "Обновление..." : "Обновить"}
                  </button>
                  <button
                    type="button"
                    onClick={handleLogout}
                    className="inline-flex items-center gap-2 rounded-xl border border-[#f08800]/40 bg-[#f08800]/12 px-4 py-2 text-sm font-semibold text-[#ffb861]"
                  >
                    <LogOut className="h-4 w-4" />
                    Сменить код
                  </button>
                </div>
              </div>
            </div>
          </section>

          {errorMessage && (
            <div className="rounded-xl border border-[#7f1d1d] bg-[#3b1212] px-4 py-3 text-sm text-[#fca5a5]">
              {errorMessage}
            </div>
          )}

          {successMessage && (
            <div className="rounded-xl border border-[#1d7f4b] bg-[#113322] px-4 py-3 text-sm text-[#86efac]">
              {successMessage}
            </div>
          )}

          <section className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
            {summaryCards.map((card) => {
              const Icon = card.visual.icon;
              return (
                <article
                  key={card.label}
                  className="group relative overflow-hidden rounded-xl border border-[#29292f] bg-[#17171a] p-4"
                >
                  <div className={`pointer-events-none absolute inset-0 bg-gradient-to-br ${card.visual.glowClassName} to-transparent opacity-0 transition-opacity duration-300 group-hover:opacity-100`} />
                  <div className="relative">
                    <div className="mb-3 flex items-start justify-between gap-2">
                      <p className="text-xs uppercase tracking-[0.13em] text-[#8f8f97]">{card.label}</p>
                      <div className={`rounded-lg border p-2 ${card.visual.iconSurfaceClassName}`}>
                        <Icon className={`h-4 w-4 ${card.visual.iconClassName}`} />
                      </div>
                    </div>
                    <p className="text-2xl font-black leading-tight text-white">{card.value}</p>
                    <p className="mt-2 text-xs text-[#b2b2bb]">{card.hint}</p>
                  </div>
                </article>
              );
            })}
          </section>

          <section className="rounded-2xl border border-[#26262b] bg-[#151518] p-4 sm:p-5">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <p className="text-xs uppercase tracking-[0.16em] text-[#8a8a90]">Клиентская база</p>
                <h2 className="mt-1 text-2xl font-black uppercase">Пользователи бота</h2>
                <p className="mt-1 text-sm text-[#a3a3ab]">Всего найдено: {totalItems}</p>
              </div>

              <div className="grid w-full gap-2 sm:grid-cols-[1fr_auto_auto] lg:w-auto lg:min-w-[640px]">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#777781]" />
                  <input
                    type="text"
                    value={searchDraft}
                    onChange={(event) => setSearchDraft(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        handleSearchSubmit();
                      }
                    }}
                    placeholder="Поиск по ID / username / серверу / привилегии"
                    className="w-full rounded-xl border border-[#2e2e35] bg-[#0f0f10] py-3 pl-9 pr-3 text-sm text-white outline-none focus:border-[#f08800]"
                  />
                </div>
                <button
                  type="button"
                  onClick={handleSearchSubmit}
                  className="rounded-xl bg-[#f08800] px-5 py-3 text-sm font-black uppercase tracking-[0.05em] text-[#111111]"
                >
                  Найти
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setSearchDraft("");
                    setSearchQuery("");
                    setPage(1);
                  }}
                  className="rounded-xl border border-[#323239] bg-[#1f1f24] px-5 py-3 text-sm font-semibold text-[#d0d0d6]"
                >
                  Сброс
                </button>
              </div>
            </div>

            <div className="mt-4 space-y-3">
              {users.map((user) => {
                const safeActivePrivileges = Array.isArray(user.activePrivileges) ? user.activePrivileges : [];
                const safeImportedPrivileges = Array.isArray(user.importedPrivileges) ? user.importedPrivileges : [];
                const safeRecentPurchases = Array.isArray(user.recentPurchases) ? user.recentPurchases : [];
                const safeImportedCount = Math.max(
                  Number(user.importedCount || safeImportedPrivileges.length || 0),
                  0,
                );
                const isUserExpanded = Boolean(expandedUsersByUser[user.userId]);

                return (
                  <article
                    key={user.userId}
                    className={`rounded-2xl border bg-[#161619] p-4 sm:p-5 ${
                      isUserExpanded ? "border-[#30303a]" : "border-[#2a2a30]"
                    }`}
                  >
                    <div
                      className="cursor-pointer"
                      role="button"
                      tabIndex={0}
                      onClick={() => toggleUserExpanded(user.userId)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          toggleUserExpanded(user.userId);
                        }
                      }}
                    >
                      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                      <div className="min-w-0 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="mt-0.5 flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-[#f08800] to-[#ca6900] text-sm font-black text-[#151515]">
                            {getUserInitial(user.displayName)}
                          </div>
                          <div className="min-w-0">
                            <div className="flex flex-wrap items-center gap-2">
                              <p className="truncate text-lg font-bold text-white">{user.displayName}</p>
                              <span className="rounded-full border border-[#2f2f35] bg-[#1f1f24] px-2 py-0.5 text-xs text-[#b3b3bb]">
                                ID: {user.userId}
                              </span>
                              {user.username && (
                                <span className="rounded-full border border-[#2563eb]/35 bg-[#1d2f53]/35 px-2 py-0.5 text-xs text-[#93c5fd]">
                                  @{user.username}
                                </span>
                              )}
                            </div>
                            <p className="mt-2 text-sm text-[#9a9aa4]">
                              Последняя активность: {formatRelative(user.lastActivityAt)}
                            </p>
                          </div>
                        </div>

                        <div className="mt-4 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-4">
                          <div className="rounded-xl border border-[#2a2a32] bg-[#101013] px-3 py-2.5 text-sm">
                            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.1em] text-[#7f7f88]">
                              <Wallet className="h-3.5 w-3.5 text-[#34d399]" />
                              Баланс
                            </div>
                            <div className="mt-1 font-semibold text-white">{formatMoney(user.balance)} UZS</div>
                          </div>
                          <div className="rounded-xl border border-[#2a2a32] bg-[#101013] px-3 py-2.5 text-sm">
                            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.1em] text-[#7f7f88]">
                              <TrendingUp className="h-3.5 w-3.5 text-[#60a5fa]" />
                              LTV
                            </div>
                            <div className="mt-1 font-semibold text-white">{formatMoney(user.ltv)} UZS</div>
                          </div>
                          <div className="rounded-xl border border-[#2a2a32] bg-[#101013] px-3 py-2.5 text-sm">
                            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.1em] text-[#7f7f88]">
                              <CreditCard className="h-3.5 w-3.5 text-[#c084fc]" />
                              Покупок
                            </div>
                            <div className="mt-1 font-semibold text-white">{user.purchaseCount}</div>
                          </div>
                          <div className="rounded-xl border border-[#2a2a32] bg-[#101013] px-3 py-2.5 text-sm">
                            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.1em] text-[#7f7f88]">
                              <Gift className="h-3.5 w-3.5 text-[#f59e0b]" />
                              Импортов
                            </div>
                            <div className="mt-1 font-semibold text-white">{safeImportedCount}</div>
                          </div>
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation();
                          openAdjustModal(user);
                        }}
                        className="h-fit rounded-xl bg-[#f08800] px-4 py-2.5 text-sm font-black uppercase tracking-[0.05em] text-[#111111]"
                      >
                        Изменить баланс
                      </button>
                    </div>
                    </div>

                    {isUserExpanded && (
                      <div className="mt-4 space-y-3">
                        <section className="rounded-xl border border-[#2a2a31] bg-[#121216] p-3">
                          <div className="flex items-center justify-between gap-2">
                            <p className="text-xs uppercase tracking-[0.12em] text-[#a9a9b2]">Активные привилегии</p>
                            <button
                              type="button"
                              onClick={() => toggleUserPrivileges(user.userId)}
                              className="rounded-lg border border-[#32323a] bg-[#1f1f24] px-2.5 py-1 text-xs font-semibold text-[#fcfcfc]"
                            >
                              {expandedPrivilegesByUser[user.userId] ? "Скрыть" : "Показать"}
                            </button>
                          </div>
                          {expandedPrivilegesByUser[user.userId] && (
                            <div className="mt-3 space-y-2">
                              {safeActivePrivileges.length === 0 ? (
                                <p className="text-sm text-[#7f7f88]">Нет активных привилегий</p>
                              ) : (
                                safeActivePrivileges.map((item) => (
                                  <div
                                    key={`${user.userId}-${item.id}-${item.serverId}`}
                                    className="rounded-lg border border-[#282830] bg-[#0e0e11] px-3 py-2 text-sm text-[#d6d6dd]"
                                  >
                                    <p className="font-semibold">
                                      {item.serverName} · {item.privilegeLabel}
                                    </p>
                                    <p className="mt-1 text-xs text-[#9e9ea8]">
                                      {item.identifierType === "steam"
                                        ? `STEAM_ID: ${item.steamId}`
                                        : `Nick: ${item.nickname}`}
                                      {item.isPermanent
                                        ? " · ∞/∞ дней"
                                        : ` · ${item.remainingDays}/${item.totalDays} дней`}
                                    </p>
                                  </div>
                                ))
                              )}
                            </div>
                          )}
                        </section>

                        <section className="rounded-xl border border-[#2a2a31] bg-[#121216] p-3">
                          <div className="flex items-center justify-between gap-2">
                            <p className="text-xs uppercase tracking-[0.12em] text-[#a9a9b2]">Импортированные привилегии</p>
                            <button
                              type="button"
                              onClick={() => toggleUserImports(user.userId)}
                              className="rounded-lg border border-[#32323a] bg-[#1f1f24] px-2.5 py-1 text-xs font-semibold text-[#fcfcfc]"
                            >
                              {expandedImportsByUser[user.userId] ? "Скрыть" : `Показать (${safeImportedCount})`}
                            </button>
                          </div>
                          {expandedImportsByUser[user.userId] && (
                            <div className="mt-3 space-y-2">
                              {safeImportedPrivileges.length === 0 ? (
                                <p className="text-sm text-[#7f7f88]">Импортов пока нет</p>
                              ) : (
                                safeImportedPrivileges.map((item) => (
                                  <div
                                    key={`${user.userId}-import-${item.id}`}
                                    className="rounded-lg border border-[#282830] bg-[#0e0e11] px-3 py-2 text-sm text-[#d6d6dd]"
                                  >
                                    <p className="flex items-center gap-2 font-semibold">
                                      <span>
                                        {item.serverName} · {item.privilege || "—"}
                                      </span>
                                      <span className="rounded-full border border-[#22c55e]/45 bg-[#052e1c] px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-[#86efac]">
                                        Legacy import
                                      </span>
                                      {item.isPermanent && (
                                        <span className="rounded-full border border-[#60a5fa]/45 bg-[#0c1f39] px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-[#93c5fd]">
                                          Постоянная
                                        </span>
                                      )}
                                    </p>
                                    <p className="mt-1 text-xs text-[#9e9ea8]">
                                      {item.identifierType === "steam"
                                        ? `STEAM_ID: ${item.steamId || "-"}`
                                        : `Nick: ${item.nickname || "-"}`}
                                      {` · ${formatDateTime(item.createdAt)}`}
                                    </p>
                                  </div>
                                ))
                              )}
                            </div>
                          )}
                        </section>

                        <section className="rounded-xl border border-[#2a2a31] bg-[#121216] p-3">
                          <div className="flex items-center justify-between gap-2">
                            <p className="text-xs uppercase tracking-[0.12em] text-[#a9a9b2]">Последние покупки</p>
                            <button
                              type="button"
                              onClick={() => toggleUserPurchases(user.userId)}
                              className="rounded-lg border border-[#32323a] bg-[#1f1f24] px-2.5 py-1 text-xs font-semibold text-[#fcfcfc]"
                            >
                              {expandedPurchasesByUser[user.userId] ? "Скрыть" : "Показать"}
                            </button>
                          </div>
                          {expandedPurchasesByUser[user.userId] && (
                            <div className="mt-3 space-y-2">
                              {safeRecentPurchases.length === 0 ? (
                                <p className="text-sm text-[#7f7f88]">История покупок пуста</p>
                              ) : (
                                safeRecentPurchases.map((purchase) => (
                                  <div
                                    key={`${user.userId}-${purchase.id}`}
                                    className="rounded-lg border border-[#282830] bg-[#0e0e11] px-3 py-2 text-sm text-[#d6d6dd]"
                                  >
                                    <p className="flex items-center gap-2 font-semibold">
                                      <span>
                                        {purchase.serverName} · {purchase.privilege || purchase.productType}
                                      </span>
                                      {String(purchase.source || "").toLowerCase() === "legacy_import" && (
                                        <span className="rounded-full border border-[#22c55e]/45 bg-[#052e1c] px-2 py-0.5 text-[10px] font-black uppercase tracking-wide text-[#86efac]">
                                          Legacy import
                                        </span>
                                      )}
                                    </p>
                                    <p className="mt-1 text-xs text-[#9e9ea8]">
                                      {formatDateTime(purchase.createdAt)} · {formatMoney(purchase.amount)} UZS
                                    </p>
                                  </div>
                                ))
                              )}
                            </div>
                          )}
                        </section>
                      </div>
                    )}
                  </article>
                );
              })}

              {isLoading && (
                <div className="rounded-xl border border-[#29292e] bg-[#101013] p-6 text-sm text-[#b8b8c0]">
                  Загрузка данных...
                </div>
              )}

              {!isLoading && users.length === 0 && (
                <div className="rounded-xl border border-[#29292e] bg-[#101013] p-6 text-sm text-[#b8b8c0]">
                  Пользователи не найдены по текущему фильтру.
                  {!searchQuery.trim() && (
                    <div className="mt-2 text-xs text-[#8a8a8f]">
                      Если это неожиданно, проверьте API выше (должен указывать на текущий backend бота).
                    </div>
                  )}
                </div>
              )}
            </div>

            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-[#26262c] pt-4">
              <p className="text-sm text-[#8e8e97]">
                Страница {page} из {totalPages}
              </p>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  disabled={page <= 1}
                  onClick={() => setPage((current) => Math.max(1, current - 1))}
                  className="rounded-lg border border-[#303036] bg-[#202024] px-4 py-2 text-sm font-semibold text-[#d0d0d6] disabled:opacity-50"
                >
                  Назад
                </button>
                <button
                  type="button"
                  disabled={page >= totalPages}
                  onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
                  className="rounded-lg border border-[#303036] bg-[#202024] px-4 py-2 text-sm font-semibold text-[#d0d0d6] disabled:opacity-50"
                >
                  Далее
                </button>
              </div>
            </div>
          </section>
        </div>
      </div>

      {adjustModal && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/70 px-4 backdrop-blur-sm">
          <div className="w-full max-w-lg space-y-4 rounded-2xl border border-[#2a2a2e] bg-[#121215] p-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.14em] text-[#8e8e97]">Корректировка</p>
                <h3 className="mt-1 text-xl font-black uppercase">Баланс пользователя</h3>
                <p className="mt-1 text-sm text-[#a4a4ad]">
                  {adjustModal.user.displayName} · ID {adjustModal.user.userId}
                </p>
              </div>
              <button
                type="button"
                onClick={closeAdjustModal}
                className="text-[#8e8e97] hover:text-[#fcfcfc]"
              >
                ✕
              </button>
            </div>

            <div className="space-y-2">
              <label className="block text-sm text-[#b6b6bf]">Сумма (можно с минусом)</label>
              <input
                type="number"
                value={adjustModal.amount}
                onChange={(event) => setAdjustModal({ ...adjustModal, amount: event.target.value, error: "" })}
                placeholder="Например: 15000 или -5000"
                className="w-full rounded-xl border border-[#2e2e33] bg-[#0f0f10] px-4 py-3 text-sm text-white outline-none focus:border-[#f08800]"
              />
            </div>

            <div className="space-y-2">
              <label className="block text-sm text-[#b6b6bf]">Комментарий (обязательно)</label>
              <textarea
                rows={3}
                value={adjustModal.comment}
                onChange={(event) => setAdjustModal({ ...adjustModal, comment: event.target.value, error: "" })}
                placeholder="Причина изменения баланса"
                className="w-full resize-none rounded-xl border border-[#2e2e33] bg-[#0f0f10] px-4 py-3 text-sm text-white outline-none focus:border-[#f08800]"
              />
            </div>

            <div className="space-y-2">
              <label className="block text-sm text-[#b6b6bf]">Метка админа</label>
              <input
                type="text"
                value={adjustModal.adminLabel}
                onChange={(event) => setAdjustModal({ ...adjustModal, adminLabel: event.target.value })}
                className="w-full rounded-xl border border-[#2e2e33] bg-[#0f0f10] px-4 py-3 text-sm text-white outline-none focus:border-[#f08800]"
              />
            </div>

            {adjustModal.error && (
              <div className="rounded-xl border border-[#7f1d1d] bg-[#3b1212] px-3 py-2 text-sm text-[#fca5a5]">
                {adjustModal.error}
              </div>
            )}

            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={closeAdjustModal}
                className="rounded-xl border border-[#303036] bg-[#232327] py-3 font-semibold text-[#d0d0d6]"
              >
                Отмена
              </button>
              <button
                type="button"
                onClick={() => {
                  void submitAdjustBalance();
                }}
                disabled={adjustModal.submitting}
                className="rounded-xl bg-[#f08800] py-3 font-black text-[#111111] disabled:opacity-60"
              >
                {adjustModal.submitting ? "Сохранение..." : "Сохранить"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
