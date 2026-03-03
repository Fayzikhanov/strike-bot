import { RouterProvider } from 'react-router-dom';
import { router } from './routes';
import { LanguageProvider } from "./i18n/LanguageContext";
import { BalanceTopUpProvider } from "./context/BalanceTopUpContext";

export default function App() {
  return (
    <LanguageProvider>
      <BalanceTopUpProvider>
        <RouterProvider router={router} />
      </BalanceTopUpProvider>
    </LanguageProvider>
  );
}
