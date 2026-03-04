import { useEffect, useState } from "react";
import { LogOut, Megaphone, Menu, PanelLeftClose, PanelLeftOpen, LayoutDashboard } from "lucide-react";
import { Navigate, NavLink, Outlet, useNavigate } from "react-router-dom";
import { clearSavedAdminKey, readSavedAdminKey } from "../../lib/adminAuth";

type AdminNavItem = {
  to: string;
  label: string;
  icon: typeof LayoutDashboard;
};

const NAV_ITEMS: AdminNavItem[] = [
  {
    to: "/admin/dashboard",
    label: "Дашборд",
    icon: LayoutDashboard,
  },
  {
    to: "/admin/broadcasts",
    label: "Рассылки",
    icon: Megaphone,
  },
];

function readSidebarCollapsed(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    return window.localStorage.getItem("strike_admin_sidebar_collapsed_v1") === "1";
  } catch {
    return false;
  }
}

function saveSidebarCollapsed(value: boolean): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem("strike_admin_sidebar_collapsed_v1", value ? "1" : "0");
  } catch {
    // ignore storage errors
  }
}

function SidebarLink({ item, collapsed }: { item: AdminNavItem; collapsed: boolean }) {
  const Icon = item.icon;
  return (
    <NavLink
      to={item.to}
      className={({ isActive }) =>
        [
          "group flex items-center gap-3 rounded-xl border px-3 py-2.5 text-sm transition-colors",
          collapsed ? "justify-center" : "",
          isActive
            ? "border-[#f08800]/50 bg-[#f08800]/12 text-[#ffb861]"
            : "border-[#2f2f36] bg-[#17171b] text-[#d2d2d9] hover:bg-[#1e1e23]",
        ].join(" ")
      }
      title={collapsed ? item.label : undefined}
    >
      <Icon className="h-4 w-4 shrink-0" />
      {!collapsed && <span className="font-semibold">{item.label}</span>}
    </NavLink>
  );
}

function MobileAdminNav({ onLogout }: { onLogout: () => void }) {
  return (
    <div className="border-b border-[#222228] bg-[#111114] p-3 md:hidden">
      <div className="flex flex-wrap items-center gap-2">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                [
                  "inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em]",
                  isActive
                    ? "border-[#f08800]/50 bg-[#f08800]/12 text-[#ffb861]"
                    : "border-[#303038] bg-[#1b1b20] text-[#d0d0d6]",
                ].join(" ")
              }
            >
              <Icon className="h-3.5 w-3.5" />
              {item.label}
            </NavLink>
          );
        })}

        <button
          type="button"
          onClick={onLogout}
          className="ml-auto inline-flex items-center gap-2 rounded-lg border border-[#f08800]/35 bg-[#f08800]/12 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-[#ffb861]"
        >
          <LogOut className="h-3.5 w-3.5" />
          Выйти
        </button>
      </div>
    </div>
  );
}

export function AdminPanelShell() {
  const navigate = useNavigate();
  const [collapsed, setCollapsed] = useState<boolean>(() => readSidebarCollapsed());
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  const adminKey = readSavedAdminKey().trim();
  if (mounted && !adminKey) {
    return <Navigate to="/admin/login" replace />;
  }

  const handleToggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    saveSidebarCollapsed(next);
  };

  const handleLogout = () => {
    clearSavedAdminKey();
    navigate("/admin/login", { replace: true });
  };

  return (
    <div className="min-h-screen bg-[#0a0a0a] text-white">
      <div className="flex min-h-screen">
        <aside
          className={[
            "hidden border-r border-[#212127] bg-[#111114] transition-all duration-300 md:flex md:flex-col",
            collapsed ? "md:w-[92px]" : "md:w-[270px]",
          ].join(" ")}
        >
          <div className="flex items-center justify-between gap-2 border-b border-[#212127] px-3 py-3">
            {!collapsed && (
              <div>
                <p className="text-[10px] uppercase tracking-[0.18em] text-[#7f7f88]">Strike.Uz</p>
                <p className="text-sm font-black uppercase">Admin Panel</p>
              </div>
            )}
            <button
              type="button"
              onClick={handleToggle}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-[#303038] bg-[#1b1b20] text-[#d2d2d9]"
              title={collapsed ? "Раскрыть меню" : "Свернуть меню"}
            >
              {collapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
            </button>
          </div>

          <nav className="flex-1 space-y-2 px-3 py-4">
            {NAV_ITEMS.map((item) => (
              <SidebarLink key={item.to} item={item} collapsed={collapsed} />
            ))}
          </nav>

          <div className="border-t border-[#212127] px-3 py-3">
            <button
              type="button"
              onClick={handleLogout}
              className={[
                "inline-flex w-full items-center rounded-xl border border-[#f08800]/35 bg-[#f08800]/12 px-3 py-2.5 text-sm font-semibold text-[#ffb861]",
                collapsed ? "justify-center" : "gap-2",
              ].join(" ")}
              title={collapsed ? "Выйти" : undefined}
            >
              <LogOut className="h-4 w-4 shrink-0" />
              {!collapsed && "Выйти"}
            </button>
          </div>
        </aside>

        <div className="min-w-0 flex-1">
          <div className="hidden border-b border-[#222228] bg-[#111114] px-4 py-2.5 md:flex md:items-center md:justify-between">
            <div className="flex items-center gap-2 text-sm text-[#8f8f97]">
              <Menu className="h-4 w-4" />
              Админ-панель
            </div>
          </div>

          <MobileAdminNav onLogout={handleLogout} />
          <Outlet />
        </div>
      </div>
    </div>
  );
}
