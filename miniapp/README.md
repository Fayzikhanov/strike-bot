# Strike.Uz Mini App

Миниапп для Telegram-бота Strike.Uz, собранный по Figma-дизайну.

## Локальный запуск

```bash
cd miniapp
npm install
npm run dev
```

Локально фронт проксирует `/api/*` на `http://127.0.0.1:8090` (см. `vite.config.ts`).

## Прод-сборка

```bash
cd miniapp
npm run build
```

Готовая статика будет в `miniapp/dist`.

## API для живых серверов и игроков

Миниапп ожидает API:
- `GET /api/servers`
- `GET /api/servers/{port}/players`

Этот API поднимается внутри `bot.py` на `API_HOST:API_PORT` (по умолчанию `0.0.0.0:8090`).

## Подключение к Telegram боту

1. Задеплойте `miniapp` (Vercel/Netlify/Nginx) по HTTPS URL.
2. В окружении бота укажите:

```bash
WEB_APP_URL=https://your-miniapp-domain.example
API_HOST=0.0.0.0
API_PORT=8090
```

3. Для прод-миниаппа укажите URL API при сборке:

```bash
VITE_API_BASE_URL=https://your-bot-api-domain.example npm run build
```

4. Перезапустите бота.

После этого кнопка `📱 App` в приватном чате будет открывать Mini App,
а список серверов/игроков в miniapp станет живым.

## Важно (для этого проекта)

Экран `Servers` и экран деталей сервера в Mini App должны использовать только live API бота (`/api/servers`, `/api/servers/{port}/players`), который получает данные через библиотеку `python-a2s`.

Не использовать fallback на статические/рандомные сервера и игроков в production miniapp.
