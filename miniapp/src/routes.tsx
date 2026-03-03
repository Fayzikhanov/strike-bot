import { createBrowserRouter } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ServersList } from "./pages/ServersList";
import { ServerDetail } from "./pages/ServerDetail";
import { Privileges } from "./pages/Privileges";
import { Purchase } from "./pages/Purchase";
import { Profile } from "./pages/Profile";

export const router = createBrowserRouter([
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
]);
