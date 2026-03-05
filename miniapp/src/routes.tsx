import { Navigate, createBrowserRouter } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ServersList } from "./pages/ServersList";
import { ServerDetail } from "./pages/ServerDetail";
import { Privileges } from "./pages/Privileges";
import { Purchase } from "./pages/Purchase";
import { Profile } from "./pages/Profile";
import { AdminDashboard } from "./pages/AdminDashboard";
import { AdminLogin } from "./pages/AdminLogin";
import { AdminBroadcasts } from "./pages/AdminBroadcasts";
import { AdminPanelShell } from "./components/admin/AdminPanelShell";

const ROUTER_BASENAME = String(import.meta.env.BASE_URL || "/")
  .replace(/\/+$/, "")
  .trim();

function AdminRootRedirect() {
  return <Navigate to="/admin/dashboard" replace />;
}

export const router = createBrowserRouter(
  [
    {
      path: "/admin/login",
      Component: AdminLogin,
    },
    {
      path: "/admin",
      Component: AdminPanelShell,
      children: [
        { index: true, Component: AdminRootRedirect },
        { path: "dashboard", Component: AdminDashboard },
        { path: "broadcasts", Component: AdminBroadcasts },
      ],
    },
    {
      path: "/",
      Component: Layout,
      children: [
        { index: true, Component: ServersList },
        { path: "server/:id", Component: ServerDetail },
        { path: "privileges", Component: Privileges },
        { path: "purchase", Component: Purchase },
        { path: "profile", Component: Profile },
      ],
    },
  ],
  {
    basename: ROUTER_BASENAME || undefined,
  },
);
